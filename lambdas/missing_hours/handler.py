import os
from datetime import datetime, timedelta, timezone

import boto3

dynamodb = boto3.client("dynamodb")
TABLE_NAME = os.environ["WATERMARK_TABLE"]
PIPELINE_ID = os.environ["PIPELINE_ID"]

MAX_BACKFILL_DAYS = 90


def lambda_handler(event, context):
    backfill = (event or {}).get("backfill")

    if backfill:
        # Manual backfill mode -- generates the requested date range
        # directly and NEVER touches DynamoDB. Safe by construction:
        # etl_job.py's own forward-only watermark guard decides afterward
        # whether any of this range actually advances the watermark.
        start = datetime.strptime(backfill["start_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end = datetime.strptime(backfill["end_date"], "%Y-%m-%d").replace(tzinfo=timezone.utc)

        if end < start:
            raise ValueError(
                f"backfill end_date ({backfill['end_date']}) is before "
                f"start_date ({backfill['start_date']})"
            )

        span_days = (end - start).days + 1
        if span_days > MAX_BACKFILL_DAYS:
            raise ValueError(
                f"backfill range spans {span_days} days, exceeding the "
                f"{MAX_BACKFILL_DAYS}-day safety cap. Split into smaller "
                f"ranges if this is genuinely intended."
            )

        missing_groups = []
        current = start
        while current <= end:
            missing_groups.append({"dt": current.strftime("%Y-%m-%d"), "hours": []})
            current += timedelta(days=1)

        print(f"Backfill mode: {span_days} day(s), {backfill['start_date']} to {backfill['end_date']}")
        return {"missing_groups": missing_groups}

    # --- Normal, watermark-driven path (calendar math, unchanged from before) ---
    response = dynamodb.get_item(
        TableName=TABLE_NAME,
        Key={"pipeline_id": {"S": PIPELINE_ID}},
    )

    item = response.get("Item")
    if item and "last_loaded_dt" in item and "last_loaded_hour" in item:
        last_dt = item["last_loaded_dt"]["S"]
        last_hour = int(item["last_loaded_hour"]["S"])
        last_loaded = datetime.strptime(f"{last_dt} {last_hour:02d}", "%Y-%m-%d %H").replace(
            tzinfo=timezone.utc
        )
    else:
        last_loaded = datetime.now(timezone.utc) - timedelta(hours=24)

    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    upper_bound = now - timedelta(hours=1)

    missing_hours = []
    cursor = last_loaded + timedelta(hours=1)
    while cursor <= upper_bound:
        missing_hours.append(cursor)
        cursor += timedelta(hours=1)

    groups = {}
    for h in missing_hours:
        groups.setdefault(h.strftime("%Y-%m-%d"), []).append(h.strftime("%H"))

    missing_groups = [{"dt": dt, "hours": hours} for dt, hours in sorted(groups.items())]

    print(f"Watermark-driven mode: {len(missing_groups)} date group(s) computed, "
          f"last_loaded was {last_loaded.isoformat()}")
    return {"missing_groups": missing_groups}