INSERT INTO stg.airports_data 
    (airport_code, airport_name, city, coordinates, timezone, created_at, updated_at) 

SELECT 
    *
FROM
    bookings.airports_data

ON CONFLICT(airport_code) 
DO UPDATE SET
    airport_name = EXCLUDED.airport_name,
    city = EXCLUDED.city,
    coordinates = EXCLUDED.coordinates,
    timezone = EXCLUDED.timezone,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;