"""
PulseGrid device simulator.

Generates hourly batch JSONL files for a mixed fleet of industrial and
smart-building sensors (HVAC units, motors, cold storage units, smart
meters), writing one file per hour under:

    <output_dir>/dt=YYYY-MM-DD/hour=HH/batch_<timestamp>.jsonl

Usage:
    python -m sensor_etl.generate --days 3 --output-dir data/sample
    python -m sensor_etl.generate --start 2026-06-01 --end 2026-08-22 --output-dir data/raw
"""

import argparse
import json
import random
from datetime import datetime, timedelta, timezone
from pathlib import Path

from sensor_etl.config import (
    DEVICE_TYPES,
    ZONES,
    FACILITIES,
    FIRMWARE_VERSIONS,
    STATUS_WEIGHTS,
    DEVICE_DROPOUT_RATE,
    BAD_DATA_RATE,
)


def build_fleet(seed: int | None = None) -> list[dict]:
    """Create the static device roster (device_id, type, facility, zone, firmware).

    seed is what keeps facility/zone/firmware assignment identical across
    every run -- don't pass a different value unless resetting it on purpose.
    """
    if seed is not None:
        random.seed(seed)

    fleet = []
    for device_type, cfg in DEVICE_TYPES.items():
        for i in range(cfg["count"]):
            fleet.append({
                "device_id": f"{device_type}-{i:03d}",
                "device_type": device_type,
                "facility_id": random.choice(FACILITIES),
                "zone": random.choice(ZONES),
                "firmware_version": random.choice(FIRMWARE_VERSIONS),
            })
    return fleet


def _sample_status() -> str:
    return random.choices(
        population=list(STATUS_WEIGHTS.keys()),
        weights=list(STATUS_WEIGHTS.values()),
        k=1,
    )[0]


def _sample_reading_fields(device_type: str) -> dict:
    cfg = DEVICE_TYPES[device_type]
    values = {}
    for field in cfg["fields"]:
        low, high = cfg["ranges"][field]
        if isinstance(low, int) and isinstance(high, int):
            values[field] = random.randint(low, high)
        else:
            values[field] = round(random.uniform(low, high), 2)
    return values


def generate_reading(device: dict, ts: datetime) -> dict:
    record = {
        "device_id": device["device_id"],
        "device_type": device["device_type"],
        "facility_id": device["facility_id"],
        "zone": device["zone"],
        "timestamp": ts.isoformat(),
        "battery_pct": round(random.uniform(15.0, 100.0), 1),
        "status_code": _sample_status(),
        "firmware_version": device["firmware_version"],
    }
    record.update(_sample_reading_fields(device["device_type"]))
    return record


def inject_bad_data(record: dict) -> dict:
    """Randomly corrupt a record to exercise the downstream data-quality gate."""
    kind = random.choice(["null_field", "out_of_range", "duplicate_marker"])
    type_fields = DEVICE_TYPES[record["device_type"]]["fields"]

    if kind == "null_field":
        field = random.choice(type_fields)
        record[field] = None
    elif kind == "out_of_range":
        field = random.choice(type_fields)
        record[field] = 999999
    else:
        # Marked so the writer knows to emit this record twice
        record["_force_duplicate"] = True

    return record


def generate_hour(fleet: list[dict], ts: datetime) -> list[dict]:
    # Reseeded per exact timestamp (date+hour), not left continuous across
    # calls -- reruns of the same hour reproduce identically, while
    # different hours/dates genuinely differ. Doesn't affect fleet assignment.
    random.seed(f"pulsegrid-{ts.isoformat()}")
    records = []
    for device in fleet:
        if random.random() < DEVICE_DROPOUT_RATE:
            continue  # device didn't report this hour

        record = generate_reading(device, ts)

        if random.random() < BAD_DATA_RATE:
            record = inject_bad_data(record)

        force_duplicate = record.pop("_force_duplicate", False)
        records.append(record)
        if force_duplicate:
            records.append(dict(record))  # duplicate device_id + timestamp

    return records


def write_batch(records: list[dict], ts: datetime, output_dir: Path) -> Path:
    partition_dir = output_dir / f"dt={ts.strftime('%Y-%m-%d')}" / f"hour={ts.strftime('%H')}"
    partition_dir.mkdir(parents=True, exist_ok=True)

    file_path = partition_dir / f"batch_{ts.strftime('%Y%m%dT%H%M%S')}.jsonl"
    with open(file_path, "w") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    return file_path


def run(start: datetime, end: datetime, output_dir: Path, seed: int | None = None) -> None:
    fleet = build_fleet(seed=seed)
    output_dir.mkdir(parents=True, exist_ok=True)

    current = start
    total_records = 0
    total_files = 0

    while current <= end:
        records = generate_hour(fleet, current)
        if records:
            write_batch(records, current, output_dir)
            total_files += 1
            total_records += len(records)
        current += timedelta(hours=1)

    print(
        f"Generated {total_files} batch files, {total_records} records, "
        f"for {len(fleet)} devices, {start.date()} to {end.date()}."
    )


def main():
    parser = argparse.ArgumentParser(description="PulseGrid device simulator")
    parser.add_argument("--output-dir", type=str, default="data/sample")
    parser.add_argument("--days", type=int, help="Generate the last N days up to now")
    parser.add_argument("--start", type=str, help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", type=str, help="End date, YYYY-MM-DD (default: today)")
    # Fixed default (not random) so facility/zone assignment stays identical
    # across every run. Override only to intentionally reset the fleet.
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    if args.days:
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(days=args.days)
    elif args.start:
        start = datetime.strptime(args.start, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        end_str = args.end or datetime.now(timezone.utc).strftime("%Y-%m-%d")
        end = datetime.strptime(end_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    else:
        parser.error("Provide either --days or --start")

    run(start, end, Path(args.output_dir), seed=args.seed)


if __name__ == "__main__":
    main()
