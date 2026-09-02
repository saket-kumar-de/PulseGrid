DELETE FROM daily_hvac_stability WHERE dt BETWEEN :start_date AND :end_date;

INSERT INTO daily_hvac_stability
SELECT CAST(dt AS DATE), device_id, facility_id, zone,
       ROUND(AVG(temperature_c), 2), ROUND(STDDEV(temperature_c), 2), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'hvac_unit' AND dt BETWEEN :start_date AND :end_date
GROUP BY dt, device_id, facility_id, zone;