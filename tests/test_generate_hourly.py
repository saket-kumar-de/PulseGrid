"""
Tests for lambdas/generate_hourly/handler.py -- the hourly device-data
simulation Lambda.

Uses moto for S3 and freezegun for "now" (same reasons as
test_missing_hours.py). Deliberately does NOT re-test generate_hour()'s
own randomness/dropout/corruption behavior -- that's already covered by
tests/test_generate.py. This file only covers what's unique to the
Lambda itself: the target_hour calculation, S3 write mechanics, key
naming, and response shape.
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
HANDLER_PATH = Path(__file__).parent.parent / "lambdas" / "generate_hourly" / "handler.py"

# So the real sensor_etl package (build_fleet/generate_hour) is importable
# when handler.py's own top-level `from sensor_etl.generate import ...` runs.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture
def handler(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    monkeypatch.setenv("RAW_BUCKET", RAW_BUCKET)

    spec = importlib.util.spec_from_file_location("generate_hourly_handler", HANDLER_PATH)
    module = importlib.util.module_from_spec(spec)
    # Same reason as test_missing_hours.py: freezegun scans sys.modules to
    # find modules needing their datetime reference patched.
    sys.modules["generate_hourly_handler"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def s3_bucket():
    with mock_aws():
        client = boto3.client("s3", region_name="ap-south-1")
        client.create_bucket(
            Bucket=RAW_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": "ap-south-1"},
        )
        yield client


def test_target_hour_is_now_minus_one_truncated(handler, s3_bucket):
    """Mirrors missing_hours' own truncation test: mid-hour 'now' (14:37)
    must truncate to 14:00, then subtract one hour -- 13:00, never the
    still-in-progress hour."""
    handler.s3 = s3_bucket

    with freeze_time("2026-08-30 14:37:00"):
        result = handler.lambda_handler({}, None)

    assert result["hour"] == "2026-08-30T13:00:00+00:00"


def test_writes_correct_s3_key_and_matching_content(handler, s3_bucket):
    """Confirms the actual S3 object lands at the right dt=/hour=/batch_
    path, and its content matches EXACTLY what generate_hour itself would
    produce for the same fleet and timestamp -- ground truth computed
    directly, not guessed at."""
    handler.s3 = s3_bucket

    from sensor_etl.generate import build_fleet, generate_hour
    from datetime import datetime, timezone

    target_hour = datetime(2026, 8, 30, 13, tzinfo=timezone.utc)
    expected_records = generate_hour(build_fleet(seed=42), target_hour)

    with freeze_time("2026-08-30 14:37:00"):
        result = handler.lambda_handler({}, None)

    assert result["records_written"] == len(expected_records)

    expected_key = "dt=2026-08-30/hour=13/batch_20260830T130000.jsonl"
    obj = s3_bucket.get_object(Bucket=RAW_BUCKET, Key=expected_key)
    body_lines = obj["Body"].read().decode("utf-8").strip().split("\n")

    assert len(body_lines) == len(expected_records)
    actual_records = [json.loads(line) for line in body_lines]
    assert actual_records == expected_records


def test_no_records_skips_s3_write_entirely(handler, s3_bucket, monkeypatch):
    """Isolates the Lambda's OWN branch logic from generate_hour's real
    randomness by forcing an empty result directly -- confirms no S3
    object gets written and the response correctly reports zero."""
    handler.s3 = s3_bucket
    monkeypatch.setattr(handler, "generate_hour", lambda fleet, ts: [])

    with freeze_time("2026-08-30 14:37:00"):
        result = handler.lambda_handler({}, None)

    assert result == {"records_written": 0, "hour": "2026-08-30T13:00:00+00:00"}

    objects = s3_bucket.list_objects_v2(Bucket=RAW_BUCKET)
    assert objects.get("KeyCount", 0) == 0