INSERT INTO stg.ticket_flights 
    (ticket_no, flight_id, fare_conditions, amount) 

SELECT
    ticket_no, 
    flight_id, 
    fare_conditions, 
    amount
FROM
    bookings.ticket_flights

ON CONFLICT(ticket_no, flight_id) 
DO UPDATE SET
    fare_conditions = EXCLUDED.fare_conditions,
    amount = EXCLUDED.amount,
    updated_at = CURRENT_TIMESTAMP;