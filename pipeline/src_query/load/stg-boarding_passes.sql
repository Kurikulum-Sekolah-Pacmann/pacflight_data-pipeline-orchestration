INSERT INTO stg.boarding_passes 
    (ticket_no, flight_id, boarding_no, seat_no) 

SELECT
    ticket_no,
    flight_id,
    boarding_no,
    seat_no
FROM
    bookings.boarding_passes

ON CONFLICT(ticket_no, flight_id) 
DO UPDATE SET
    boarding_no = EXCLUDED.boarding_no,
    seat_no = EXCLUDED.seat_no,
    updated_at = CURRENT_TIMESTAMP;