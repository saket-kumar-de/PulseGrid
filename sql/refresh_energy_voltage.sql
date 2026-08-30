DELETE FROM daily_energy_voltage_summary WHERE dt = CAST(:target_date AS DATE);

INSERT INTO daily_energy_voltage_summary
SELECT CAST(dt AS DATE), device_id, facility_id, SUM(energy_kwh), AVG(voltage), MIN(voltage), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'smart_meter' AND dt = :target_date
GROUP BY dt, device_id, facility_id;