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
    updated_at = CASE WHEN 
                        stg.aircrafts_data.model <> EXCLUDED.model
                        OR stg.aircrafts_data.range <> EXCLUDED.range 
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.aircrafts_data.updated_at
                END;