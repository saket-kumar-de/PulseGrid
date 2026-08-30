DELETE FROM daily_hvac_stability WHERE dt = CAST(:target_date AS DATE);

INSERT INTO daily_hvac_stability
SELECT CAST(dt AS DATE), device_id, facility_id, zone, AVG(temperature_c), STDDEV(temperature_c), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'hvac_unit' AND dt = :target_date
GROUP BY dt, device_id, facility_id, zone;