import sys
import json
import time
from datetime import datetime, timezone

import boto3
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType
from pyspark.sql.window import Window
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(
    sys.argv,
    ["JOB_NAME", "raw_database", "raw_table", "curated_bucket", "target_date"],
)


# JOB_RUN_ID is deliberately NOT requested via getResolvedOptions above --
# awsglue's parser treats it as an internally-reserved name and errors with
# "conflicting option string" if you also list it yourself. Extracted
# manually instead, with a fallback in case it isn't present under this
# exact flag.
def _extract_arg(flag_name):
    flag = f"--{flag_name}"
    if flag in sys.argv:
        idx = sys.argv.index(flag)
        return sys.argv[idx + 1]
    return None


job_run_id = _extract_arg("JOB_RUN_ID") or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%f")

# hours is optional -- parsed the same manual way as JOB_RUN_ID above, for
# the same reserved-name reason.
if "--hours" in sys.argv:
    idx = sys.argv.index("--hours")
    hours = json.loads(sys.argv[idx + 1])
else:
    hours = None

start_time = time.time()

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Required for idempotent reruns: only writes touching dt/hour partitions
# present in THIS run's output get replaced, not the whole dataset.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
target_date = args["target_date"]

# Whole day, or specific hours within it (catch-up case)
if hours:
    hour_list_sql = ", ".join(f"'{h}'" for h in hours)
    predicate = f"dt = '{target_date}' AND hour IN ({hour_list_sql})"
else:
    predicate = f"dt = '{target_date}'"

numeric_cols = ["battery_pct", "temperature_c", "humidity_pct", "vibration_mm_s",
                 "rpm", "door_open_count", "energy_kwh", "voltage"]


def write_audit_record(clean_count=0, quarantine_count=0, note=None):
    # Self-reported only on success -- a crashed job never reaches this,
    # so failure records are written by the orchestrator instead, from
    # the outside. JOB_RUN_ID in the key means reruns never collide.
    record = {
        "run_id": job_run_id,
        "target_date": target_date,
        "hours_processed": hours if hours else "ALL",
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


raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database=args["raw_database"],
    table_name=args["raw_table"],
    push_down_predicate=predicate,
)

# push_down_predicate matching ZERO partitions (e.g. every requested hour
# genuinely has no data) leaves the DynamicFrame with no inferable schema
# at all -- not just zero rows. Downstream named-column transforms would
# fail on this. Handled as an if/else (not sys.exit) since Glue's job
# runner treats ANY raised exception, including SystemExit(0), as a
# failed run regardless of the exit code -- both branches must converge
# on the same job.commit() at the bottom instead.
if raw_dyf.count() == 0:
    write_audit_record(note="no matching raw data found for the requested date/hours")
else:
    # Some numeric fields mix ints (corrupted 999999 sentinel) and floats
    # (normal readings) across records -- Glue represents this as an
    # ambiguous "choice" struct rather than a plain double. resolveChoice
    # collapses it to double BEFORE converting to a DataFrame, where a
    # plain .cast() can't handle it.
    raw_dyf = raw_dyf.resolveChoice(specs=[(c, "cast:double") for c in numeric_cols])
    df = raw_dyf.toDF()

    # Raw keeps timestamp as a string (schema-on-read); cast to real types here
    df = df.withColumn("event_ts", F.to_timestamp("timestamp"))

    for c in numeric_cols:
        df = df.withColumn(c, F.col(c).cast(DoubleType()))

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
        # OR's together per-device-type range checks: a row only trips a check
        # if it's that device type AND that field is outside its valid range.
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

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    # Clean -> Parquet (columnar, efficient for later Athena/Redshift queries)
    clean_df.write.mode("overwrite").partitionBy("dt", "hour") \
        .parquet(f"s3://{args['curated_bucket']}/sensor_readings/")

    # Quarantined -> JSON, deliberately human-readable for manual inspection
    quarantine_df.write.mode("overwrite").partitionBy("dt", "hour") \
        .json(f"s3://{args['curated_bucket']}/quarantine/sensor_readings/")

    df.unpersist()

    write_audit_record(clean_count=clean_count, quarantine_count=quarantine_count)

job.commit()