INSERT INTO stg.seats 
    (aircraft_code, seat_no, fare_conditions, created_at, updated_at) 

SELECT
    *
FROM
    bookings.seats

ON CONFLICT(aircraft_code, seat_no) 
DO UPDATE SET
    fare_conditions = EXCLUDED.fare_conditions,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;