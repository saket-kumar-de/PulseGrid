"""
Tests for lambdas/generate_hourly/handler.py -- the hourly device-data
simulation Lambda.

Uses moto for S3 and Glue, and freezegun for "now" (same reasons as
test_missing_hours.py). Deliberately does NOT re-test generate_hour()'s
own randomness/dropout/corruption behavior -- that's already covered by
tests/test_generate.py. This file only covers what's unique to the
Lambda itself: the target_hour calculation, S3 write mechanics, key
naming, response shape, and real partition registration.

The handler now calls glue.get_table() at MODULE IMPORT time (not just
at invocation time) to read the raw table's real StorageDescriptor and
partition-key order -- so the Glue mock and a seeded fake table must
already be active BEFORE exec_module runs, not patched in afterward the
way the s3 client used to be. This is why the fixture creates the fake
database/table first, then execs the module inside that same active
mock_aws() context.
"""
import importlib.util
import json
import sys
from pathlib import Path

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_aws

RAW_BUCKET = "pulsegrid-dev-raw"
RAW_DATABASE = "pulsegrid_dev_raw_db"
RAW_TABLE = "pulsegrid_dev_raw"
HANDLER_PATH = Path(__file__).parent.parent / "lambdas" / "generate_hourly" / "handler.py"

# So the real sensor_etl package (build_fleet/generate_hour) is importable
# when handler.py's own top-level `from sensor_etl.generate import ...` runs.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture
def handler(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    monkeypatch.setenv("RAW_BUCKET", RAW_BUCKET)
    monkeypatch.setenv("RAW_DATABASE", RAW_DATABASE)
    monkeypatch.setenv("RAW_TABLE", RAW_TABLE)

    with mock_aws():
        s3 = boto3.client("s3", region_name="ap-south-1")
        s3.create_bucket(
            Bucket=RAW_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-south-1"},
        )

        # A minimal but real fake table -- genuinely mirrors the real
        # deployed raw table's partition-key order (dt, then hour), so
        # handler.py's own key-order-derivation logic is exercised
        # honestly, not just given an order it happens to already assume.
        glue = boto3.client("glue", region_name="ap-south-1")
        glue.create_database(DatabaseInput={"Name": RAW_DATABASE})
        glue.create_table(
            DatabaseName=RAW_DATABASE,
            TableInput={
                "Name": RAW_TABLE,
                "StorageDescriptor": {
                    "Columns": [{"Name": "device_id", "Type": "string"}],
                    "Location": f"s3://{RAW_BUCKET}/",
                    "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
                    "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    "SerdeInfo": {"SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe"},
                },
                "PartitionKeys": [
                    {"Name": "dt", "Type": "string"},
                    {"Name": "hour", "Type": "string"},
                ],
            },
        )

        spec = importlib.util.spec_from_file_location("generate_hourly_handler", HANDLER_PATH)
        module = importlib.util.module_from_spec(spec)
        # Same reason as test_missing_hours.py: freezegun scans sys.modules to
        # find modules needing their datetime reference patched.
        sys.modules["generate_hourly_handler"] = module
        spec.loader.exec_module(module)

        # module.s3 and module.glue were both created inside this same
        # active mock_aws() context during exec_module above, so they're
        # already the correctly-mocked clients -- no post-hoc reassignment
        # needed, unlike the pre-registration version of this fixture.
        yield module


def test_target_hour_is_now_minus_one_truncated(handler):
    """Mirrors missing_hours' own truncation test: mid-hour 'now' (14:37)
    must truncate to 14:00, then subtract one hour -- 13:00, never the
    still-in-progress hour."""
    with freeze_time("2026-08-30 14:37:00"):
        result = handler.lambda_handler({}, None)

    assert result["hour"] == "2026-08-30T13:00:00+00:00"


def test_writes_correct_s3_key_and_matching_content(handler):
    """Confirms the actual S3 object lands at the right dt=/hour=/batch_
    path, and its content matches EXACTLY what generate_hour itself would
    produce for the same fleet and timestamp -- ground truth computed
    directly, not guessed at."""
    from sensor_etl.generate import build_fleet, generate_hour
    from datetime import datetime, timezone

    target_hour = datetime(2026, 8, 30, 13, tzinfo=timezone.utc)
    expected_records = generate_hour(build_fleet(seed=42), target_hour)

    with freeze_time("2026-08-30 14:37:00"):
        result = handler.lambda_handler({}, None)

    assert result["records_written"] == len(expected_records)

    expected_key = "dt=2026-08-30/hour=13/batch_20260830T130000.jsonl"
    obj = handler.s3.get_object(Bucket=RAW_BUCKET, Key=expected_key)
    body_lines = obj["Body"].read().decode("utf-8").strip().split("\n")

    assert len(body_lines) == len(expected_records)
    actual_records = [json.loads(line) for line in body_lines]
    assert actual_records == expected_records


def test_partition_is_registered_in_glue_catalog(handler):
    """Confirms the handler's real partition-registration call actually
    lands a real, queryable partition entry -- not just that the write
    succeeded, but that Spectrum-style catalog lookups would genuinely
    find it afterward."""
    with freeze_time("2026-08-30 14:37:00"):
        handler.lambda_handler({}, None)

    partitions = handler.glue.get_partitions(DatabaseName=RAW_DATABASE, TableName=RAW_TABLE)
    values = [p["Values"] for p in partitions["Partitions"]]
    assert ["2026-08-30", "13"] in values


def test_no_records_skips_s3_write_entirely(handler, monkeypatch):
    """Isolates the Lambda's OWN branch logic from generate_hour's real
    randomness by forcing an empty result directly -- confirms no S3
    object gets written and the response correctly reports zero."""
    monkeypatch.setattr(handler, "generate_hour", lambda fleet, ts: [])

    with freeze_time("2026-08-30 14:37:00"):
        result = handler.lambda_handler({}, None)

    assert result == {"records_written": 0, "hour": "2026-08-30T13:00:00+00:00"}

    objects = handler.s3.list_objects_v2(Bucket=RAW_BUCKET)
    assert objects.get("KeyCount", 0) == 0