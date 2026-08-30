-- DISTSTYLE ALL: small dimension-like rollups, replicated to every node.
-- SORTKEY(dt): every query filters by date.

DROP TABLE IF EXISTS daily_device_health_summary;
CREATE TABLE daily_device_health_summary (
    dt DATE, device_type VARCHAR(30), facility_id VARCHAR(50),
    clean_reading_count BIGINT, quarantined_reading_count BIGINT, total_reading_count BIGINT,
    dq_failure_rate_pct DECIMAL(5,2), avg_battery_pct DECIMAL(10,2), min_battery_pct DECIMAL(10,2)
) DISTSTYLE ALL SORTKEY (dt);

DROP TABLE IF EXISTS daily_motor_vibration_trend;
CREATE TABLE daily_motor_vibration_trend (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50),
    avg_vibration_mm_s DECIMAL(10,2), max_vibration_mm_s DECIMAL(10,2), reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);

DROP TABLE IF EXISTS daily_cold_storage_risk;
CREATE TABLE daily_cold_storage_risk (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50),
    avg_temperature_c DECIMAL(10,2), total_door_open_count BIGINT, reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);

DROP TABLE IF EXISTS daily_hvac_stability;
CREATE TABLE daily_hvac_stability (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50), zone VARCHAR(50),
    avg_temperature_c DECIMAL(10,2), stddev_temperature_c DECIMAL(10,2), reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);

DROP TABLE IF EXISTS daily_energy_voltage_summary;
CREATE TABLE daily_energy_voltage_summary (
    dt DATE, device_id VARCHAR(50), facility_id VARCHAR(50),
    total_energy_kwh DECIMAL(10,2), avg_voltage DECIMAL(10,2), min_voltage DECIMAL(10,2), reading_count BIGINT
) DISTSTYLE ALL SORTKEY (dt);