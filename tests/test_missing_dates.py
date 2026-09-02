"""
Tests for lambdas/missing_dates/handler.py -- the redshift_refresh
missing-dates Lambda.

Uses moto to mock DynamoDB. The handler module is imported fresh inside a
fixture (not at file scope) so required env vars can be set first, since
the handler reads them at import time. Loaded via its exact file path
(not a plain `import`) since missing_hours/missing_dates/generate_hourly
all share the literal filename handler.py.
"""
import importlib.util
from pathlib import Path

import boto3
import pytest
from moto import mock_aws

TABLE_NAME = "pulsegrid-dev-watermarks"
HANDLER_PATH = Path(__file__).parent.parent / "lambdas" / "missing_dates" / "handler.py"


@pytest.fixture
def handler(monkeypatch):
    monkeypatch.setenv("AWS_DEFAULT_REGION", "ap-south-1")
    monkeypatch.setenv("WATERMARK_TABLE", TABLE_NAME)
    monkeypatch.setenv("PIPELINE_ID", "redshift_refresh")
    monkeypatch.setenv("UPSTREAM_PIPELINE_ID", "sensor_etl")

    spec = importlib.util.spec_from_file_location("missing_dates_handler", HANDLER_PATH)
    module = importlib.util.module_from_spec(spec)
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


def test_watermark_driven_mode_computes_correct_range(handler, dynamodb_table):
    """sensor_etl mid-day (hour!=23): safe_upper_bound excludes today;
    redshift_refresh behind: start_date is the day after last_aggregated_dt."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "sensor_etl"},
        "last_loaded_dt": {"S": "2026-09-01"}, "last_loaded_hour": {"S": "14"},
    })
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "redshift_refresh"},
        "last_aggregated_dt": {"S": "2026-08-26"},
    })

    result = handler.lambda_handler({}, None)

    assert result == {"start_date": "2026-08-27", "end_date": "2026-08-31"}


def test_fully_closed_day_included_when_hour_is_23(handler, dynamodb_table):
    """sensor_etl at hour=23: that day itself is genuinely safe to include."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "sensor_etl"},
        "last_loaded_dt": {"S": "2026-09-01"}, "last_loaded_hour": {"S": "23"},
    })
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "redshift_refresh"},
        "last_aggregated_dt": {"S": "2026-08-26"},
    })

    result = handler.lambda_handler({}, None)

    assert result == {"start_date": "2026-08-27", "end_date": "2026-09-01"}


def test_already_caught_up_returns_empty(handler, dynamodb_table):
    """redshift_refresh already at the safe upper bound: nothing to do."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "sensor_etl"},
        "last_loaded_dt": {"S": "2026-09-01"}, "last_loaded_hour": {"S": "23"},
    })
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "redshift_refresh"},
        "last_aggregated_dt": {"S": "2026-09-01"},
    })

    result = handler.lambda_handler({}, None)

    assert result == {}


def test_no_upstream_watermark_returns_empty(handler, dynamodb_table):
    """sensor_etl has never run: nothing is safe to aggregate yet."""
    handler.dynamodb = dynamodb_table

    result = handler.lambda_handler({}, None)

    assert result == {}


def test_backfill_mode_ignores_watermarks_entirely(handler, dynamodb_table):
    """backfill bypasses DynamoDB completely -- returns exactly the
    requested range regardless of either watermark's real state."""
    handler.dynamodb = dynamodb_table
    dynamodb_table.put_item(TableName=TABLE_NAME, Item={
        "pipeline_id": {"S": "redshift_refresh"},
        "last_aggregated_dt": {"S": "2026-09-01"},
    })

    result = handler.lambda_handler(
        {"backfill": {"start_date": "2026-06-01", "end_date": "2026-06-03"}}, None
    )

    assert result == {"start_date": "2026-06-01", "end_date": "2026-06-03"}


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