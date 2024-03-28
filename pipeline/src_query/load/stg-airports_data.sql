INSERT INTO stg.airports_data 
    (airport_code, airport_name, city, coordinates, timezone) 

SELECT 
    airport_code, 
    airport_name, 
    city, 
    coordinates, 
    timezone
FROM
    bookings.airports_data

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
                        CURRENT_TIMESTAMP
                ELSE
                        stg.airports_data.updated_at
                END;