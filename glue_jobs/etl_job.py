import sys
import json
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, [
    "JOB_NAME", "raw_database", "raw_table", "curated_bucket",
    "curated_database", "watermark_table",
])

# Spectrum reads Glue table partition METADATA, never partition-projection
# table properties -- that's an Athena-only feature. So every partition
# this job writes must be explicitly registered, the same way generate_hourly
# already does for raw. These three names are structural constants, not
# runtime-parameterized, mirroring generate_hourly's own RAW_TABLE choice.
CURATED_TABLE = "sensor_readings"
QUARANTINE_TABLE = "quarantine_sensor_readings"
PIPELINE_RUNS_TABLE = "pipeline_runs"


def _extract_arg(flag_name):
    flag = f"--{flag_name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        return sys.argv[idx + 1]
    return None


job_run_id = _extract_arg("JOB_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")
run_timestamp = datetime.now(timezone.utc).isoformat()

# date_groups replaces target_date/hours: a JSON list of {"dt": "...", "hours": [...]}
# (empty hours list = whole day). One Spark session processes every group in
# the list -- one Glue cold start regardless of how many dates are requested.
date_groups = json.loads(_extract_arg("date_groups"))

start_time = time.time()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

dynamodb = boto3.client("dynamodb")
glue = boto3.client("glue")
WATERMARK_TABLE = args["watermark_table"]
CURATED_DATABASE = args["curated_database"]

# Each table's own StorageDescriptor and partition-key order, read ONCE and
# reused for every partition registered this run -- guarantees consistency
# with the table's real definition rather than reconstructing one here, and
# derives Values ordering from the table itself rather than assuming dt-then-
# hour. Same reasoning as generate_hourly's own cold-start table read.
def _load_table_meta(table_name):
    t = glue.get_table(DatabaseName=CURATED_DATABASE, Name=table_name)["Table"]
    return t["StorageDescriptor"], [k["Name"] for k in t["PartitionKeys"]]

_curated_sd, _curated_key_order = _load_table_meta(CURATED_TABLE)
_quarantine_sd, _quarantine_key_order = _load_table_meta(QUARANTINE_TABLE)
_pipeline_runs_sd, _pipeline_runs_key_order = _load_table_meta(PIPELINE_RUNS_TABLE)


def register_partitions(table_name, table_sd, key_order, location_prefix, dt_hour_pairs):
    """Registers a batch of (dt, hour) partitions via BatchCreatePartition,
    chunked to its 100-per-call limit. AlreadyExistsException entries in the
    response are expected on a reprocess/backfill and logged, not raised --
    same as generate_hourly's single-item skip, just via the batch API's own
    Errors-list response shape rather than a raised exception."""
    pairs = sorted(dt_hour_pairs)
    for i in range(0, len(pairs), 100):
        chunk = pairs[i:i + 100]
        partition_inputs = []
        for dt, hour in chunk:
            values_by_name = {"dt": dt, "hour": hour}
            sd = dict(table_sd)
            sd["Location"] = f"{location_prefix}dt={dt}/hour={hour}/"
            partition_inputs.append({
                "Values": [values_by_name[k] for k in key_order],
                "StorageDescriptor": sd,
            })
        response = glue.batch_create_partition(
            DatabaseName=CURATED_DATABASE, TableName=table_name,
            PartitionInputList=partition_inputs,
        )
        real_errors = []
        for err in response.get("Errors", []):
            if err["ErrorDetail"]["ErrorCode"] == "AlreadyExistsException":
                print(f"{table_name}: partition {err['PartitionValues']} already "
                      f"registered, skipping")
            else:
                real_errors.append(err)
        if real_errors:
            raise RuntimeError(f"{table_name}: partition registration errors: {real_errors}")
    print(f"{table_name}: registered {len(pairs)} partition(s)")


def register_pipeline_runs_partition(target_date):
    """pipeline_runs is partitioned by dt only -- one call per audit record,
    called from inside write_audit_record right after its own S3 write."""
    values_by_name = {"dt": target_date}
    sd = dict(_pipeline_runs_sd)
    sd["Location"] = f"s3://{args['curated_bucket']}/audit/pipeline_runs/dt={target_date}/"
    try:
        glue.create_partition(
            DatabaseName=CURATED_DATABASE, TableName=PIPELINE_RUNS_TABLE,
            PartitionInput={
                "Values": [values_by_name[k] for k in _pipeline_runs_key_order],
                "StorageDescriptor": sd,
            },
        )
        print(f"{PIPELINE_RUNS_TABLE}: registered partition dt={target_date}")
    except glue.exceptions.AlreadyExistsException:
        print(f"{PIPELINE_RUNS_TABLE}: partition dt={target_date} already registered, skipping")


# Build one OR-of-ANDs predicate covering every requested date/hour combination
group_predicates = []
for group in date_groups:
    dt = group["dt"]
    hours = group.get("hours")
    if hours:
        hour_list_sql = ", ".join(f"'{h}'" for h in hours)
        group_predicates.append(f"(dt = '{dt}' AND hour IN ({hour_list_sql}))")
    else:
        group_predicates.append(f"(dt = '{dt}')")
predicate = " OR ".join(group_predicates)

numeric_cols = ["battery_pct", "temperature_c", "humidity_pct", "vibration_mm_s",
                 "rpm", "door_open_count", "energy_kwh", "voltage"]


def write_audit_record(target_date, hours_processed, clean_count=0, quarantine_count=0, note=None):
    # Self-reported only on success -- a crashed job never reaches this, so
    # failure records are written by the orchestrator instead, from the
    # outside. JOB_RUN_ID + target_date in the key means reruns never collide.
    record = {
        "run_id": job_run_id,
        "run_timestamp": run_timestamp,
        "target_date": target_date,
        "hours_processed": hours_processed,
        "status": "SUCCESS",
        "started_at": datetime.fromtimestamp(start_time, tz=timezone.utc).isoformat(),
        "completed_at": datetime.now(timezone.utc).isoformat(),
        "duration_seconds": round(time.time() - start_time, 1),
        "records_clean": clean_count,
        "records_quarantined": quarantine_count,
    }
    if note:
        record["note"] = note
    boto3.client("s3").put_object(
        Bucket=args["curated_bucket"],
        Key=f"audit/pipeline_runs/dt={target_date}/run_{job_run_id}.json",
        Body=json.dumps(record).encode("utf-8"),
    )
    register_pipeline_runs_partition(target_date)


def advance_watermark(new_dt, new_hour):
    # Atomic, server-side forward-only guard -- no GetItem needed, no race
    # window, since the comparison happens entirely inside DynamoDB's own
    # condition evaluation, not in this script's Python.
    new_key = f"{new_dt}T{new_hour}"
    try:
        dynamodb.update_item(
            TableName=WATERMARK_TABLE,
            Key={"pipeline_id": {"S": "sensor_etl"}},
            UpdateExpression="SET last_loaded_dt = :dt, last_loaded_hour = :hour, last_loaded_key = :key",
            ConditionExpression="attribute_not_exists(last_loaded_key) OR last_loaded_key < :key",
            ExpressionAttributeValues={
                ":dt": {"S": new_dt}, ":hour": {"S": new_hour}, ":key": {"S": new_key},
            },
        )
        print(f"Watermark advanced to {new_key}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            print(f"Watermark NOT advanced: {new_key} is not newer than the "
                  f"current watermark. Left unchanged (likely a manual reprocess).")
        else:
            raise


raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["raw_database"], table_name=args["raw_table"], push_down_predicate=predicate,
)

# push_down_predicate matching ZERO partitions across the WHOLE combined
# request leaves the DynamicFrame with no inferable schema at all. Handled
# as an if/else (NOT sys.exit) since Glue's job runner treats ANY raised
# exception, including SystemExit(0), as a failed run regardless of the
# exit code -- both branches must converge on job.commit() at the bottom.
if raw_dyf.count() == 0:
    print("No matching raw data found for any requested date/hour combination. "
          "No audit records written, watermark left untouched.")
else:
    # Some numeric fields mix ints (corrupted 999999 sentinel) and floats
    # (normal readings) across records -- Glue represents this as an
    # ambiguous "choice" struct rather than a plain double. resolveChoice
    # collapses it to double BEFORE converting to a DataFrame, where a
    # plain .cast() can't handle it.
    raw_dyf = raw_dyf.resolveChoice(specs=[(c, "cast:double") for c in numeric_cols])
    df = raw_dyf.toDF()
    df = df.withColumn("event_ts", F.to_timestamp("timestamp"))
    for c in numeric_cols:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))

    # The chronologically-latest dt/hour actually FOUND in the raw data --
    # not requested, FOUND. This is what the watermark advances to, and
    # what caps which requested dates are allowed an audit record at all.
    max_row = df.agg(F.max(F.concat_ws("T", "dt", "hour")).alias("max_key")).collect()[0]
    run_max_dt, run_max_hour = max_row["max_key"].split("T")

    # --- DQ gate: flags the 3 corruption modes the simulator injects --
    # null required fields, out-of-range values, and exact duplicates.
    # Mirrors sensor_etl/config.py's valid ranges exactly.
    RANGES = {
        "hvac_unit":         [("temperature_c", 18.0, 26.0), ("humidity_pct", 30.0, 60.0)],
        "motor":             [("vibration_mm_s", 0.5, 4.5), ("rpm", 800, 3600)],
        "cold_storage_unit": [("temperature_c", -25.0, -10.0), ("door_open_count", 0, 5)],
        "smart_meter":       [("energy_kwh", 0.1, 15.0), ("voltage", 215.0, 245.0)],
    }

    def out_of_range_expr():
        # OR's together per-device-type range checks: a row only trips a
        # check if it's that device type AND that field is outside its
        # valid range.
        expr = F.lit(False)
        for device_type, checks in RANGES.items():
            for col, lo, hi in checks:
                bad = (F.col("device_type") == device_type) & F.col(col).isNotNull() & \
                      ((F.col(col) < lo) | (F.col(col) > hi))
                expr = expr | bad
        return expr

    # A row is bad if any required field is null
    REQUIRED_COLS = ["device_id", "device_type", "event_ts", "battery_pct"]
    null_check = F.lit(False)
    for c in REQUIRED_COLS:
        null_check = null_check | F.col(c).isNull()

    # Flags rows whose (device_id, event_ts) pair appears more than once
    dup_window = Window.partitionBy("device_id", "event_ts")
    df = df.withColumn("_dup", F.count("*").over(dup_window) > 1)
    df = df.withColumn("_null", null_check)
    df = df.withColumn("_range", out_of_range_expr())
    df = df.withColumn("_is_bad", F.col("_dup") | F.col("_null") | F.col("_range"))

    # Cache once so the counts and both writes below reuse this instead of
    # each separately re-reading and re-transforming from raw
    df.cache()

    clean_df = df.filter(~F.col("_is_bad")).drop("_dup", "_null", "_range", "_is_bad", "timestamp")
    quarantine_df = df.filter(F.col("_is_bad")).drop("_dup", "_null", "_range", "_is_bad")

    # Per-date counts, not one overall scalar -- so each date still gets
    # its own queryable audit record, even though every date shared one
    # Spark session.
    clean_counts = {row["dt"]: row["count"] for row in clean_df.groupBy("dt").count().collect()}
    quarantine_counts = {row["dt"]: row["count"] for row in quarantine_df.groupBy("dt").count().collect()}

    # Distinct (dt, hour) pairs each DataFrame actually produced -- computed
    # separately, since an hour can be clean-only, quarantine-only, or both.
    clean_partitions = {(row["dt"], row["hour"]) for row in clean_df.select("dt", "hour").distinct().collect()}
    quarantine_partitions = {(row["dt"], row["hour"]) for row in quarantine_df.select("dt", "hour").distinct().collect()}

    # Clean -> Parquet (columnar, efficient for later Athena/Redshift queries)
    clean_df.write.mode("overwrite").partitionBy("dt", "hour") \
        .parquet(f"s3://{args['curated_bucket']}/sensor_readings/")

    # Quarantined -> JSON, deliberately human-readable for manual inspection
    quarantine_df.write.mode("overwrite").partitionBy("dt", "hour") \
        .json(f"s3://{args['curated_bucket']}/quarantine/sensor_readings/")

    df.unpersist()

    # Register every partition actually written -- Spectrum needs real
    # catalog entries, since it never reads partition-projection properties
    # at all (an Athena-only feature). Runs AFTER both writes succeed, never
    # before, so a registered partition never points at data that isn't
    # there yet.
    if clean_partitions:
        register_partitions(
            CURATED_TABLE, _curated_sd, _curated_key_order,
            f"s3://{args['curated_bucket']}/sensor_readings/", clean_partitions,
        )
    if quarantine_partitions:
        register_partitions(
            QUARANTINE_TABLE, _quarantine_sd, _quarantine_key_order,
            f"s3://{args['curated_bucket']}/quarantine/sensor_readings/", quarantine_partitions,
        )

    # Only dates <= what this run actually confirmed get an audit record --
    # a requested date beyond run_max_dt was never verified as processed,
    # so no record is written claiming it was, even a "0 records" one.
    for group in date_groups:
        dt = group["dt"]
        if dt <= run_max_dt:
            write_audit_record(
                target_date=dt,
                hours_processed=",".join(group["hours"]) if group.get("hours") else "ALL",
                clean_count=clean_counts.get(dt, 0),
                quarantine_count=quarantine_counts.get(dt, 0),
            )

    advance_watermark(run_max_dt, run_max_hour)

job.commit()