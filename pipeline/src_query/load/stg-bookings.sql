INSERT INTO stg.bookings 
    (book_ref, book_date, total_amount) 

SELECT
    book_ref,
    book_date,
    total_amount
FROM
    bookings.bookings

ON CONFLICT(book_ref) 
DO UPDATE SET
    book_date = EXCLUDED.book_date,
    total_amount = EXCLUDED.total_amount,
    updated_at = CURRENT_TIMESTAMP;