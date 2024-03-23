INSERT INTO stg.bookings 
    (book_ref, book_date, total_amount, created_at, updated_at) 

SELECT
    *
FROM
    bookings.bookings
    
ON CONFLICT(book_ref) 
DO UPDATE SET
    book_date = EXCLUDED.book_date,
    total_amount = EXCLUDED.total_amount,
    updated_at = CASE WHEN 
                        stg.bookings.book_date <> EXCLUDED.book_date
                        OR stg.bookings.total_amount <> EXCLUDED.total_amount 
                THEN 
                        CURRENT_TIMESTAMP
                ELSE
                        stg.bookings.updated_at
                END;