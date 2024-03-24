INSERT INTO stg.boarding_passes 
    (ticket_no, flight_id, boarding_no, seat_no, created_at, updated_at) 

SELECT
    *
FROM
    bookings.boarding_passes

ON CONFLICT(ticket_no, flight_id) 
DO UPDATE SET
    boarding_no = EXCLUDED.boarding_no,
    seat_no = EXCLUDED.seat_no,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;