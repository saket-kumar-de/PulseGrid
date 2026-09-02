import os
from datetime import datetime, timedelta, timezone

import boto3

dynamodb = boto3.client("dynamodb")
TABLE_NAME = os.environ["WATERMARK_TABLE"]
PIPELINE_ID = os.environ["PIPELINE_ID"]
UPSTREAM_PIPELINE_ID = os.environ["UPSTREAM_PIPELINE_ID"]

MAX_BACKFILL_DAYS = 90


def lambda_handler(event, context):
    backfill = (event or {}).get("backfill")

    if backfill:
        # Manual backfill mode -- generates the requested range directly and
        # NEVER touches DynamoDB. Mirrors missing-hours' own backfill branch.
        start = backfill["start_date"]
        end = backfill["end_date"]
        start_dt = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_dt = datetime.strptime(end, "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if end_dt < start_dt:
            raise ValueError(f"backfill end_date ({end}) is before start_date ({start})")
        span_days = (end_dt - start_dt).days + 1
        if span_days > MAX_BACKFILL_DAYS:
            raise ValueError(
                f"backfill range spans {span_days} days, exceeding the "
                f"{MAX_BACKFILL_DAYS}-day safety cap."
            )

        print(f"Backfill mode: {span_days} day(s), {start} to {end}")
        return {"start_date": start, "end_date": end}

    # --- Normal, watermark-driven path ---
    # Read sensor_etl's watermark to find the last FULLY closed date -- never
    # aggregate a date sensor_etl hasn't completely finished loading yet.
    upstream = dynamodb.get_item(
        TableName=TABLE_NAME, Key={"pipeline_id": {"S": UPSTREAM_PIPELINE_ID}}
    ).get("Item")

    if not upstream or "last_loaded_dt" not in upstream:
        print("sensor_etl has no watermark yet -- nothing safe to aggregate.")
        return {}

    sensor_dt = upstream["last_loaded_dt"]["S"]
    sensor_hour = upstream["last_loaded_hour"]["S"]

    if sensor_hour == "23":
        safe_upper_bound = sensor_dt
    else:
        safe_upper_bound = (
            datetime.strptime(sensor_dt, "%Y-%m-%d") - timedelta(days=1)
        ).strftime("%Y-%m-%d")

    # Read redshift_refresh's own watermark
    own = dynamodb.get_item(
        TableName=TABLE_NAME, Key={"pipeline_id": {"S": PIPELINE_ID}}
    ).get("Item")

    if own and "last_aggregated_dt" in own:
        last_aggregated = own["last_aggregated_dt"]["S"]
        start_date = (
            datetime.strptime(last_aggregated, "%Y-%m-%d") + timedelta(days=1)
        ).strftime("%Y-%m-%d")
    else:
        # No watermark yet -- fallback only, matches missing-hours' own
        # "no item yet" convention. Real bootstrapping should seed this
        # explicitly to the true known state, same as sensor_etl's was.
        start_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    if start_date > safe_upper_bound:
        print(f"Nothing to aggregate: start_date ({start_date}) is after safe_upper_bound ({safe_upper_bound}).")
        return {}

    print(f"Watermark-driven mode: {start_date} to {safe_upper_bound}")
    return {"start_date": start_date, "end_date": safe_upper_bound}