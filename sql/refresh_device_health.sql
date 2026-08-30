DELETE FROM daily_device_health_summary WHERE dt = CAST(:target_date AS DATE);

INSERT INTO daily_device_health_summary
WITH clean_agg AS (
    SELECT dt, device_type, facility_id,
           COUNT(*) AS clean_reading_count,
           AVG(battery_pct) AS avg_battery_pct,
           MIN(battery_pct) AS min_battery_pct
    FROM curated_spectrum.sensor_readings
    WHERE dt = :target_date
    GROUP BY dt, device_type, facility_id
),
quarantine_agg AS (
    SELECT dt, device_type, facility_id, COUNT(*) AS quarantined_reading_count
    FROM curated_spectrum.quarantine_sensor_readings
    WHERE dt = :target_date
    GROUP BY dt, device_type, facility_id
)
SELECT
    CAST(COALESCE(c.dt, q.dt) AS DATE), COALESCE(c.device_type, q.device_type), COALESCE(c.facility_id, q.facility_id),
    COALESCE(c.clean_reading_count, 0), COALESCE(q.quarantined_reading_count, 0),
    COALESCE(c.clean_reading_count, 0) + COALESCE(q.quarantined_reading_count, 0),
    ROUND(
        CAST(COALESCE(q.quarantined_reading_count, 0) AS DOUBLE PRECISION) * 100
            / NULLIF(COALESCE(c.clean_reading_count, 0) + COALESCE(q.quarantined_reading_count, 0), 0),
        2
    ),
    ROUND(c.avg_battery_pct, 2), c.min_battery_pct
FROM clean_agg c FULL OUTER JOIN quarantine_agg q
    ON c.dt = q.dt AND c.device_type = q.device_type AND c.facility_id = q.facility_id;