INSERT INTO stg.flights 
    (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival, created_at, updated_at) 

SELECT 
    *
FROM
    bookings.flights

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
    created_at = EXCLUDED.created_at,
    updated_at = EXCLUDED.updated_at;