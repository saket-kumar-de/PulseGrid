# PulseGrid sensor ETL

A batch data pipeline for a mixed fleet of industrial and smart-building
sensors, built as a personal data engineering portfolio project. Simulated
devices (HVAC units, motors, cold storage units, smart meters) generate
hourly telemetry, which lands in S3, gets cleaned and validated by a Glue
ETL job, and is queried through Athena and Redshift.

## Status

Work in progress. Device simulator and base infrastructure (S3, IAM) are in
place; Glue, Redshift, and orchestration are next.

## Quick start

    python3 -m venv .venv
    source .venv/bin/activate
    pip install -e ".[dev]"
    python -m sensor_etl.generate --days 3 --output-dir data/sample
    pytest

## Fleet

| Device type       | Count | Type-specific fields              |
| ------------------ | ----- | ---------------------------------- |
| hvac_unit          | 15    | temperature_c, humidity_pct        |
| motor              | 15    | vibration_mm_s, rpm                |
| cold_storage_unit  | 15    | temperature_c, door_open_count     |
| smart_meter        | 15    | energy_kwh, voltage                |

Shared fields: `device_id`, `device_type`, `facility_id`, `zone`,
`timestamp`, `battery_pct`, `status_code`, `firmware_version`

About 4% of records are deliberately corrupted (null fields, out-of-range
values, duplicate device_id + timestamp pairs) to give the downstream data
quality gate something real to catch.

## Infrastructure

Terraform in `terraform/` provisions the S3 raw and curated buckets and the
IAM role used by Glue.

    cd terraform
    terraform init
    terraform plan
    terraform apply

## Repository structure

    src/sensor_etl/     # simulator, config, (later) transform/load logic
    terraform/           # S3 buckets, IAM role
    glue_jobs/            # Glue ETL job scripts
    step_functions/       # orchestration state machine definition
    sql/                  # Redshift DDL, merge procedures
    tests/                # pytest unit tests
    docs/                 # architecture, data dictionary, setup guide
    data/sample/           # small sample batches for local development

## Roadmap

- [x] Device simulator
- [x] Terraform base (S3, IAM)
- [ ] Glue Crawler + Data Catalog
- [ ] Glue ETL job (validation, transform)
- [ ] Redshift + Athena
- [ ] Step Functions orchestration + EventBridge schedule
- [ ] CI (GitHub Actions)
- [ ] Docs (architecture, data dictionary)
