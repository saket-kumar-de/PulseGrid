DELETE FROM daily_motor_vibration_trend WHERE dt = CAST(:target_date AS DATE);

INSERT INTO daily_motor_vibration_trend
SELECT CAST(dt AS DATE), device_id, facility_id, AVG(vibration_mm_s), MAX(vibration_mm_s), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'motor' AND dt = :target_date
GROUP BY dt, device_id, facility_id;