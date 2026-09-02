DELETE FROM daily_energy_voltage_summary WHERE dt BETWEEN :start_date AND :end_date;

INSERT INTO daily_energy_voltage_summary
SELECT CAST(dt AS DATE), device_id, facility_id,
       ROUND(SUM(energy_kwh), 2), ROUND(AVG(voltage), 2), MIN(voltage), COUNT(*)
FROM curated_spectrum.sensor_readings
WHERE device_type = 'smart_meter' AND dt BETWEEN :start_date AND :end_date
GROUP BY dt, device_id, facility_id;