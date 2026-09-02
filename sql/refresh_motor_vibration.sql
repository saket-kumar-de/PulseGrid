DELETE FROM daily_motor_vibration_trend WHERE dt BETWEEN :start_date AND :end_date;

INSERT INTO daily_motor_vibration_trend
SELECT CAST(dt AS DATE), device_id, facility_id,
       ROUND(AVG(vibration_mm_s), 2), MAX(vibration_mm_s), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'motor' AND dt BETWEEN :start_date AND :end_date
GROUP BY dt, device_id, facility_id;