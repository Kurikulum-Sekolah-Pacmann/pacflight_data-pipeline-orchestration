INSERT INTO stg.aircrafts_data 
    (aircraft_code, model, range, created_at, updated_at) 

SELECT
    *
FROM bookings.aircrafts_data

ON CONFLICT(aircraft_code) 
DO UPDATE SET
    model = EXCLUDED.model,
    range = EXCLUDED.range,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;