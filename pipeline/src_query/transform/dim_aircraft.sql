INSERT INTO final.dim_aircraft (
    aircraft_id,
    aircraft_nk,
    model,
    range, 
    created_at,
    updated_at
)

SELECT
    ad.id AS aircraft_id,
    ad.aircraft_code AS aircraft_nk,
    ad.model,
    ad.range,
    ad.created_at,
    ad.updated_at
	
FROM
    stg.aircrafts_data ad 
    
ON CONFLICT(aircraft_id) 
DO UPDATE SET
    aircraft_nk = EXCLUDED.aircraft_nk,
    model = EXCLUDED.model,
    range = EXCLUDED.range,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;