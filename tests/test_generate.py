from datetime import datetime, timezone

from sensor_etl.generate import build_fleet, generate_hour


def test_fleet_size():
    fleet = build_fleet(seed=1)
    assert len(fleet) == 60


def test_fleet_has_all_device_types():
    fleet = build_fleet(seed=1)
    types = {d["device_type"] for d in fleet}
    assert types == {"hvac_unit", "motor", "cold_storage_unit", "smart_meter"}


def test_generate_hour_produces_records():
    fleet = build_fleet(seed=1)
    ts = datetime(2026, 1, 1, tzinfo=timezone.utc)
    records = generate_hour(fleet, ts)
    assert len(records) > 0
    assert all("device_id" in r and "timestamp" in r for r in records)
