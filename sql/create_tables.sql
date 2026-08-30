-- DISTSTYLE ALL: small dimension-like rollups, replicated to every node.
-- SORTKEY(dt): every query filters by date.

CREATE TABLE IF NOT EXISTS daily_device_health_summary (
    dt DATE, device_type VARCHAR(30), facility_id VARCHAR(50),
    clean_reading_count BIGINT, quarantined_reading_count BIGINT, total_reading_count BIGINT,
    dq_failure_rate DOUBLE PRECISION, avg_battery_pct DOUBLE PRECISION, min_battery_pct DOUBLE PRECISION
) DISTSTYLE ALL SORTKEY (dt);

CREATE TABLE IF NOT EXISTS daily_motor_vibration_trend (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50),
    avg_vibration_mm_s DOUBLE PRECISION, max_vibration_mm_s DOUBLE PRECISION, reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);

CREATE TABLE IF NOT EXISTS daily_cold_storage_risk (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50),
    avg_temperature_c DOUBLE PRECISION, total_door_open_count BIGINT, reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);

CREATE TABLE IF NOT EXISTS daily_hvac_stability (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50), zone VARCHAR(50),
    avg_temperature_c DOUBLE PRECISION, stddev_temperature_c DOUBLE PRECISION, reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);

CREATE TABLE IF NOT EXISTS daily_energy_voltage_summary (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50),
    total_energy_kwh DOUBLE PRECISION, avg_voltage DOUBLE PRECISION, min_voltage DOUBLE PRECISION, reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);