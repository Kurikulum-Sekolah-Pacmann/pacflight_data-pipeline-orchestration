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
    updated_at = CASE WHEN 
                        stg.ticket_flights.fare_conditions <> EXCLUDED.fare_conditions 
                        OR stg.ticket_flights.amount <> EXCLUDED.amount
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.ticket_flights.updated_at
                END;