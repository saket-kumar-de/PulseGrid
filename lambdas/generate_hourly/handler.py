import os
import json
from datetime import datetime, timedelta, timezone

import boto3

from sensor_etl.generate import build_fleet, generate_hour

s3 = boto3.client("s3")
glue = boto3.client("glue")
RAW_BUCKET = os.environ["RAW_BUCKET"]
RAW_DATABASE = os.environ["RAW_DATABASE"]
RAW_TABLE = os.environ["RAW_TABLE"]

# Fleet built once per cold start, reused across warm invocations within the
# same execution environment -- safe, since seed=42 is fixed and untouched
# by generate_hour's own per-timestamp reseed.
fleet = build_fleet(seed=42)

# The table's own StorageDescriptor and partition-key ORDER are read ONCE at
# cold start and reused for every partition we register. This guarantees
# each new partition's format/serde/columns stays byte-for-byte consistent
# with the table's real definition, and derives the correct Values ordering
# from the table itself rather than assuming dt-then-hour.
_table = glue.get_table(DatabaseName=RAW_DATABASE, Name=RAW_TABLE)["Table"]
_table_sd = _table["StorageDescriptor"]
_partition_key_order = [k["Name"] for k in _table["PartitionKeys"]]


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

    dt_str = target_hour.strftime("%Y-%m-%d")
    hour_str = target_hour.strftime("%H")
    key = (
        f"dt={dt_str}/hour={hour_str}/"
        f"batch_{target_hour.strftime('%Y%m%dT%H%M%S')}.jsonl"
    )
    body = "\n".join(json.dumps(r) for r in records) + "\n"

    s3.put_object(Bucket=RAW_BUCKET, Key=key, Body=body.encode("utf-8"))

    partition_values = {"dt": dt_str, "hour": hour_str}
    partition_sd = dict(_table_sd)
    partition_sd["Location"] = f"s3://{RAW_BUCKET}/dt={dt_str}/hour={hour_str}/"

    try:
        glue.create_partition(
            DatabaseName=RAW_DATABASE,
            TableName=RAW_TABLE,
            PartitionInput={
                "Values": [partition_values[k] for k in _partition_key_order],
                "StorageDescriptor": partition_sd,
            },
        )
        print(f"Registered partition dt={dt_str}/hour={hour_str}")
    except glue.exceptions.AlreadyExistsException:
        # A retry for an hour already registered -- not an error, the
        # partition genuinely already exists correctly.
        print(f"Partition dt={dt_str}/hour={hour_str} already registered, skipping")

    print(f"Wrote {len(records)} records for {target_hour.isoformat()} to s3://{RAW_BUCKET}/{key}")
    return {"records_written": len(records), "hour": target_hour.isoformat()}