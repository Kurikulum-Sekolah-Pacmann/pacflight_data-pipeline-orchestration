INSERT INTO stg.aircrafts_data 
    (aircraft_code, model, range, created_at, updated_at) 
VALUES 
    ('{aircraft_code}', '{model}', '{range}', '{created_at}', '{updated_at}')
ON CONFLICT(aircraft_code) 
DO UPDATE SET
    model = EXCLUDED.model,
    range = EXCLUDED.range,
    updated_at = CASE WHEN 
                        stg.aircrafts_data.model <> EXCLUDED.model 
                        OR stg.aircrafts_data.range <> EXCLUDED.range 
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.aircrafts_data.updated_at
                END;