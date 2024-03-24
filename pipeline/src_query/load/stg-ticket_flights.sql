INSERT INTO stg.ticket_flights 
    (ticket_no, flight_id, fare_conditions, amount, created_at, updated_at) 

SELECT
    *
FROM
    bookings.ticket_flights

ON CONFLICT(ticket_no, flight_id) 
DO UPDATE SET
    fare_conditions = EXCLUDED.fare_conditions,
    amount = EXCLUDED.amount,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;