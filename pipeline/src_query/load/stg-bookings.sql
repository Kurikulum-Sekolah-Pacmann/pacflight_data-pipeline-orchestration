INSERT INTO stg.bookings 
    (book_ref, book_date, total_amount, created_at, updated_at) 
VALUES 
    ('{book_ref}', '{book_date}', '{total_amount}', '{created_at}', '{updated_at}')
ON CONFLICT(book_ref) 
DO UPDATE SET
    book_date = EXCLUDED.book_date,
    total_amount = EXCLUDED.total_amount,
    updated_at = CASE WHEN 
                        stg.bookings.book_date <> EXCLUDED.book_date
                        OR stg.bookings.total_amount <> EXCLUDED.total_amount 
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.bookings.updated_at
                END;