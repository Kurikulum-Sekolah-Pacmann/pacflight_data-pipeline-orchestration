INSERT INTO stg.tickets 
    (ticket_no, book_ref, passenger_id, passenger_name, contact_data, created_at, updated_at) 

SELECT
    *
FROM
    bookings.tickets

ON CONFLICT(ticket_no) 
DO UPDATE SET
    book_ref = EXCLUDED.book_ref,
    passenger_id = EXCLUDED.passenger_id,
    passenger_name = EXCLUDED.passenger_name,
    contact_data = EXCLUDED.contact_data,
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;