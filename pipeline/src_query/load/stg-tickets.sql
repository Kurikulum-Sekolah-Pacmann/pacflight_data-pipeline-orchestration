INSERT INTO stg.tickets 
    (ticket_no, book_ref, passenger_id, passenger_name, contact_data, created_at, updated_at) 
VALUES 
    ('{ticket_no}', '{book_ref}', '{passenger_id}', '{passenger_name}', '{contact_data}', '{created_at}', '{updated_at}')
ON CONFLICT(ticket_no) 
DO UPDATE SET
    book_ref = EXCLUDED.book_ref,
    passenger_id = EXCLUDED.passenger_id,
    passenger_name = EXCLUDED.passenger_name,
    contact_data = EXCLUDED.contact_data,
    updated_at = CASE WHEN 
                        stg.tickets.book_ref <> EXCLUDED.book_ref 
                        OR stg.tickets.passenger_id <> EXCLUDED.passenger_id 
                        OR stg.tickets.passenger_name <> EXCLUDED.passenger_name
                        OR stg.tickets.contact_data <> EXCLUDED.contact_data
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.tickets.updated_at
                END;