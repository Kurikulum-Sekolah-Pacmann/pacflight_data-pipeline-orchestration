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
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;