DELETE FROM daily_cold_storage_risk WHERE dt BETWEEN :start_date AND :end_date;

INSERT INTO daily_cold_storage_risk
SELECT CAST(dt AS DATE), device_id, facility_id,
       ROUND(AVG(temperature_c), 2), SUM(door_open_count), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'cold_storage_unit' AND dt BETWEEN :start_date AND :end_date
GROUP BY dt, device_id, facility_id;