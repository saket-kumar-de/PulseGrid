import os
from datetime import datetime, timedelta, timezone

import boto3

dynamodb = boto3.client("dynamodb")
TABLE_NAME = os.environ["WATERMARK_TABLE"]
PIPELINE_ID = os.environ["PIPELINE_ID"]


def lambda_handler(event, context):
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
        # No watermark item yet -- fallback only to 24 hours ago, to avoid accidentally reprocessing too much data.
        last_loaded = datetime.now(timezone.utc) - timedelta(hours=24)

    # Never touch the current, possibly-still-accumulating hour.
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    upper_bound = now - timedelta(hours=1)

    missing_hours = []
    cursor = last_loaded + timedelta(hours=1)
    while cursor <= upper_bound:
        missing_hours.append(cursor)
        cursor += timedelta(hours=1)

    # Group by date for the Step Functions Map state -- one Glue job run per group.
    groups = {}
    for h in missing_hours:
        groups.setdefault(h.strftime("%Y-%m-%d"), []).append(h.strftime("%H"))

    return {"missing_groups": [{"dt": dt, "hours": hours, "last_hour": hours[-1]} for dt, hours in sorted(groups.items())]}