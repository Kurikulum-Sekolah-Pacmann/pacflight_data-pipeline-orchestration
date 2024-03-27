INSERT INTO final.dim_airport (
    airport_id,
    airport_nk,
    airport_name,
    city,
    coordinates,
    timezone
)

SELECT
    ad.id AS airport_id,
    ad.airport_code AS airport_nk,
    ad.airport_name,
    ad.city->>'en' AS city,
    ad.coordinates,
    ad.timezone
	
FROM
    stg.airports_data ad 
    
ON CONFLICT(airport_id) 
DO UPDATE SET
    airport_nk = EXCLUDED.airport_nk,
    airport_name = EXCLUDED.airport_name,
    city = EXCLUDED.city,
    coordinates = EXCLUDED.coordinates,
    timezone = EXCLUDED.timezone,
    updated_at = CURRENT_TIMESTAMP;
               