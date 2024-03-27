INSERT INTO stg.seats 
    (aircraft_code, seat_no, fare_conditions) 

SELECT
    aircraft_code, 
    seat_no, 
    fare_conditions
FROM
    bookings.seats

ON CONFLICT(aircraft_code, seat_no) 
DO UPDATE SET
    fare_conditions = EXCLUDED.fare_conditions,
    updated_at = CURRENT_TIMESTAMP;