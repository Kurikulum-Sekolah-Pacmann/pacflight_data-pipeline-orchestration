INSERT INTO final.dim_passenger (
    passenger_id,
    passenger_nk,
    passenger_name,
    phone,
    email,
    created_at,
    updated_at
)

SELECT
    t.id AS passenger_id,
	t.passenger_id AS passenger_nk,
	t.passenger_name,
	t.contact_data->>'phone' AS phone,
    t.contact_data->>'email' AS email,
    t.created_at,
    t.updated_at
	
FROM
    stg.tickets t 
    
ON CONFLICT(passenger_id) 
DO UPDATE SET
    passenger_nk = EXCLUDED.passenger_nk,
    passenger_name = EXCLUDED.passenger_name,
    phone = EXCLUDED.phone,
    email = EXCLUDED.email,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;