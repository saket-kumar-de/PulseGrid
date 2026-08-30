DELETE FROM daily_cold_storage_risk WHERE dt = CAST(:target_date AS DATE);

INSERT INTO daily_cold_storage_risk
SELECT CAST(dt AS DATE), device_id, facility_id, AVG(temperature_c), SUM(door_open_count), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'cold_storage_unit' AND dt = :target_date
GROUP BY dt, device_id, facility_id;