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
    updated_at = CURRENT_TIMESTAMP;