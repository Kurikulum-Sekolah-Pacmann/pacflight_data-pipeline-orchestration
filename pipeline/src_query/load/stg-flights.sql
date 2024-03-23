INSERT INTO stg.flights 
    (flight_id, flight_no, scheduled_departure, scheduled_arrival, departure_airport, arrival_airport, status, aircraft_code, actual_departure, actual_arrival) 
VALUES 
    ('{flight_id}', '{flight_no}', '{scheduled_departure}', '{scheduled_arrival}', '{departure_airport}', '{arrival_airport}', '{status}', '{aircraft_code}', {actual_departure}, {actual_arrival})
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
                        stg.flights.flight_no <> EXCLUDED.flight_no
                        OR stg.flights.scheduled_departure <> EXCLUDED.scheduled_departure 
                        OR stg.flights.scheduled_arrival <> EXCLUDED.scheduled_arrival
                        OR stg.flights.departure_airport <> EXCLUDED.departure_airport
                        OR stg.flights.arrival_airport <> EXCLUDED.arrival_airport
                        OR stg.flights.status <> EXCLUDED.status
                        OR stg.flights.aircraft_code <> EXCLUDED.aircraft_code
                        OR stg.flights.actual_departure <> EXCLUDED.actual_departure
                        OR stg.flights.actual_arrival <> EXCLUDED.actual_arrival
                THEN 
                        '{current_local_time}'
                ELSE
                        stg.flights.updated_at
                END;