INSERT INTO stg.airports_data 
    (airport_code, airport_name, city, coordinates, timezone, created_at, updated_at) 
VALUES 
    ('{airport_code}', '{airport_name}', '{city}', '{coordinates}', '{timezone}', '{created_at}', '{updated_at}')
ON CONFLICT(airport_code) 
DO UPDATE SET
    airport_name = EXCLUDED.airport_name,
    city = EXCLUDED.city,
    coordinates = EXCLUDED.coordinates,
    timezone = EXCLUDED.timezone,
    updated_at = CASE WHEN 
                        stg.airports_data.airport_name <> EXCLUDED.airport_name 
                        OR stg.airports_data.city <> EXCLUDED.city 
                        OR stg.airports_data.coordinates <> EXCLUDED.coordinates
                        OR stg.airports_data.timezone <> EXCLUDED.timezone
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.airports_data.updated_at
                END;