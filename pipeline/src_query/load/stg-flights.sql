INSERT INTO stg.bookings 
    (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival, created_at, updated_at) 
VALUES 
    ('{flight_id}', '{flight_no}', '{scheduled_departure}', '{scheduled_arrival}', '{departure_airport}', '{arrival_airport}', '{status}', '{aircraft_code}', '{actual_departure}', '{actual_arrival}', '{created_at}', '{updated_at}')
ON CONFLICT(flight_id) 
DO UPDATE SET
    flight_no = EXCLUDED.flight_no,
    scheduled_departure = EXCLUDED.scheduled_departure,
    scheduled_arrival = EXCLUDED.scheduled_arrival,
    departure_airport = EXCLUDED.departure_airport,
    arrival_airport = EXCLUDED.arrival_airport,
    status = EXCLUDED.status,
    aircraft_code = EXCLUDED.aircraft_code,
    actual_departure = EXCLUDED.actual_departure,
    actual_arrival = EXCLUDED.actual_arrival,
    updated_at = CASE WHEN 
                        stg.bookings.flight_no <> EXCLUDED.flight_no
                        OR stg.bookings.scheduled_departure <> EXCLUDED.scheduled_departure 
                        OR stg.bookings.scheduled_arrival <> EXCLUDED.scheduled_arrival
                        OR stg.bookings.departure_airport <> EXCLUDED.departure_
                        OR stg.bookings.arrival_airport <> EXCLUDED.arrival_airport
                        OR stg.bookings.status <> EXCLUDED.status
                        OR stg.bookings.aircraft_code <> EXCLUDED.aircraft_code
                        OR stg.bookings.actual_departure <> EXCLUDED.actual_departure
                        OR stg.bookings.actual_arrival <> EXCLUDED.actual_arrival
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.bookings.updated_at
                END;