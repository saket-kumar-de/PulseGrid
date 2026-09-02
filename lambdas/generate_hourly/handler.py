import os
import json
from datetime import datetime, timedelta, timezone

import boto3

from sensor_etl.generate import build_fleet, generate_hour

s3 = boto3.client("s3")
RAW_BUCKET = os.environ["RAW_BUCKET"]

# Fleet built once per cold start, reused across warm invocations within the
# same execution environment -- safe, since seed=42 is fixed and untouched
# by generate_hour's own per-timestamp reseed.
fleet = build_fleet(seed=42)


def lambda_handler(event, context):
    # Generate for the hour that just completed, not the one in progress --
    # mirrors missing-hours' own "never touch the still-accumulating hour"
    # rule, applied here on the producer side instead of the consumer side.
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    target_hour = now - timedelta(hours=1)

    records = generate_hour(fleet, target_hour)

    if not records:
        print(f"No records generated for {target_hour.isoformat()} (all devices dropped out)")
        return {"records_written": 0, "hour": target_hour.isoformat()}

    key = (
        f"dt={target_hour.strftime('%Y-%m-%d')}/hour={target_hour.strftime('%H')}/"
        f"batch_{target_hour.strftime('%Y%m%dT%H%M%S')}.jsonl"
    )
    body = "\n".join(json.dumps(r) for r in records) + "\n"

    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body.encode("utf-8"))

    print(f"Wrote {len(records)} records for {target_hour.isoformat()} to s3://{RAW_BUCKET}/{key}")
    return {"records_written": len(records), "hour": target_hour.isoformat()}