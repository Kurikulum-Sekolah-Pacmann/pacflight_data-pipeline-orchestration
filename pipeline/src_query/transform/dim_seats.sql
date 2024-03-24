WITH stg_dim_seats AS (
    SELECT
        s.id AS seat_id,
        da.aircraft_id AS aircraft_id,
        s.seat_no,
        s.fare_conditions,
        LEAST(MIN(da.created_at), MIN(s.created_at)) AS created_at,
        GREATEST(MAX(da.updated_at), MAX(s.updated_at)) AS updated_at
    FROM
        stg.seats s

    JOIN final.dim_aircraft da 
        ON da.aircraft_nk = s.aircraft_code
        
    GROUP BY
        s.id,
        da.aircraft_id,
        s.seat_no,
        s.fare_conditions
)

INSERT INTO final.dim_seat (
    seat_id,
    aircraft_id,
    seat_no,
    fare_conditions,
    created_at,
    updated_at
)

SELECT
    seat_id,
    aircraft_id,
    seat_no,
    fare_conditions,
    created_at,
    updated_at
    
FROM
    stg_dim_seats sds
    
ON CONFLICT(seat_id) 
DO UPDATE SET
    aircraft_id = EXCLUDED.aircraft_id,
    seat_no = EXCLUDED.seat_no,
    fare_conditions = EXCLUDED.fare_conditions,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;