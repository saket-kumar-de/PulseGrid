"""
Tests for lambdas/missing_hours/handler.py -- the sensor_etl missing-hours
Lambda.

Uses moto for DynamoDB and freezegun to control "now", since the
watermark-driven path calls datetime.now() directly -- without freezing,
assertions on its output would only be correct on the exact day they were
written. backfill mode never touches real time, so those tests don't need
freezing at all.
"""
import importlib.util
from pathlib import Path

import boto3
import pytest
from freezegun import freeze_time
from moto import mock_aws

TABLE_NAME = "pulsegrid-dev-watermarks"
HANDLER_PATH = Path(__file__).parent.parent / "lambdas" / "missing_hours" / "handler.py"


@pytest.fixture
def handler(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    monkeypatch.setenv("WATERMARK_TABLE", TABLE_NAME)
    monkeypatch.setenv("PIPELINE_ID", "sensor_etl")

    spec = importlib.util.spec_from_file_location("missing_hours_handler", HANDLER_PATH)
    module = importlib.util.module_from_spec(spec)
    import sys
    sys.modules["missing_hours_handler"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def dynamodb_table():
    with mock_aws():
        client = boto3.client("dynamodb", region_name="ap-south-1")
        client.create_table(
            TableName=TABLE_NAME,
            KeySchema=[{"AttributeName": "pipeline_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "pipeline_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        yield client


def test_single_missing_hour(handler, dynamodb_table):
    """Watermark 2 hours behind a frozen 'now' -> exactly one missing hour."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "sensor_etl"},
        "last_loaded_dt": {"S": "2026-08-30"}, "last_loaded_hour": {"S": "12"},
    })

    with freeze_time("2026-08-30 15:30:00"):
        result = handler.lambda_handler({}, None)

    assert result == {"missing_groups": [{"dt": "2026-08-30", "hours": ["13", "14"]}]}


def test_current_partial_hour_never_included(handler, dynamodb_table):
    """Frozen 'now' mid-hour (10:15) must truncate to 10:00, then exclude
    it entirely -- upper_bound is 09:00, never the in-progress hour."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "sensor_etl"},
        "last_loaded_dt": {"S": "2026-08-30"}, "last_loaded_hour": {"S": "08"},
    })

    with freeze_time("2026-08-30 10:15:00"):
        result = handler.lambda_handler({}, None)

    assert result == {"missing_groups": [{"dt": "2026-08-30", "hours": ["09"]}]}


def test_gap_spanning_a_day_boundary_groups_correctly(handler, dynamodb_table):
    """A gap crossing midnight must produce two separate date groups,
    correctly split and sorted."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "sensor_etl"},
        "last_loaded_dt": {"S": "2026-08-31"}, "last_loaded_hour": {"S": "22"},
    })

    with freeze_time("2026-09-01 02:00:00"):
        result = handler.lambda_handler({}, None)

    assert result == {"missing_groups": [
        {"dt": "2026-08-31", "hours": ["23"]},
        {"dt": "2026-09-01", "hours": ["00", "01"]},
    ]}


def test_already_caught_up_returns_empty_list(handler, dynamodb_table):
    """Watermark already at the safe upper bound -- nothing missing.
    Note: this Lambda returns an empty LIST ({"missing_groups": []}), not
    an empty dict -- a genuine structural difference from missing_dates,
    which returns {} entirely. Both are correct for their own state
    machine's IsPresent check, just shaped differently."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "sensor_etl"},
        "last_loaded_dt": {"S": "2026-08-30"}, "last_loaded_hour": {"S": "09"},
    })

    with freeze_time("2026-08-30 10:00:00"):
        result = handler.lambda_handler({}, None)

    assert result == {"missing_groups": []}


def test_no_watermark_falls_back_to_24_hours(handler, dynamodb_table):
    """No item in DynamoDB at all -- falls back to (now - 24h). The gap
    between a 24h-ago fallback and a 1h-ago upper bound is always exactly
    23 hours, regardless of what 'now' actually is."""
    handler.dynamodb = dynamodb_table
    # Deliberately no put_item call -- table exists but has no watermark yet.

    with freeze_time("2026-08-30 05:00:00"):
        result = handler.lambda_handler({}, None)

    total_hours = sum(len(g["hours"]) for g in result["missing_groups"])
    assert total_hours == 23
    assert result["missing_groups"][0]["dt"] == "2026-08-29"
    assert result["missing_groups"][-1]["hours"][-1] == "04"


def test_backfill_mode_returns_correct_groups(handler, dynamodb_table):
    """backfill never touches real time or DynamoDB -- deterministic
    regardless of when the test actually runs."""
    handler.dynamodb = dynamodb_table

    result = handler.lambda_handler(
        {"backfill": {"start_date": "2026-06-01", "end_date": "2026-06-03"}}, None
    )

    assert result == {"missing_groups": [
        {"dt": "2026-06-01", "hours": []},
        {"dt": "2026-06-02", "hours": []},
        {"dt": "2026-06-03", "hours": []},
    ]}


def test_backfill_rejects_reversed_date_range(handler, dynamodb_table):
    with pytest.raises(ValueError, match="before start_date"):
        handler.lambda_handler(
            {"backfill": {"start_date": "2026-06-05", "end_date": "2026-06-01"}}, None
        )


def test_backfill_rejects_range_exceeding_90_day_cap(handler, dynamodb_table):
    with pytest.raises(ValueError, match="90-day safety cap"):
        handler.lambda_handler(
            {"backfill": {"start_date": "2026-01-01", "end_date": "2026-04-01"}}, None
        )