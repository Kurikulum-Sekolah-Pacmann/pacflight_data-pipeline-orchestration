INSERT INTO stg.aircrafts_data 
    (aircraft_code, model, range) 

SELECT
    aircraft_code,
    model,
    range
FROM bookings.aircrafts_data

ON CONFLICT(aircraft_code) 
DO UPDATE SET
    model = EXCLUDED.model,
    range = EXCLUDED.range,
    updated_at = CURRENT_TIMESTAMP;