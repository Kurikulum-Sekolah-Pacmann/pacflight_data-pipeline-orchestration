CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CREATE SCHEMA FOR STAGING & FINAL(production)
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS prod AUTHORIZATION postgres;

------------------------------------------------------------------------------------------------------------------------------ STAGING SCHEMA
COMMENT ON SCHEMA stg IS 'Airlines demo database schema';

-- DROP SEQUENCE stg.flights_flight_id_seq;

CREATE SEQUENCE stg.flights_flight_id_seq
	MINVALUE 0
	NO MAXVALUE
	START 0
	NO CYCLE;

-- Permissions

ALTER SEQUENCE stg.flights_flight_id_seq OWNER TO postgres;
GRANT ALL ON SEQUENCE stg.flights_flight_id_seq TO postgres;
-- stg.aircrafts_data definition

-- Drop table

-- DROP TABLE stg.aircrafts_data;

CREATE TABLE stg.aircrafts_data (
	aircraft_code bpchar(3) NOT NULL, -- Aircraft code, IATA
	model jsonb NOT NULL, -- Aircraft model
	"range" int4 NOT NULL, -- Maximal flying distance, km
	created_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	updated_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	CONSTRAINT aircrafts_pkey PRIMARY KEY (aircraft_code),
	CONSTRAINT aircrafts_range_check CHECK ((range > 0))
);
COMMENT ON TABLE stg.aircrafts_data IS 'Aircrafts (internal data)';

-- Column comments

COMMENT ON COLUMN stg.aircrafts_data.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.aircrafts_data.model IS 'Aircraft model';
COMMENT ON COLUMN stg.aircrafts_data."range" IS 'Maximal flying distance, km';

-- Permissions

ALTER TABLE stg.aircrafts_data OWNER TO postgres;
GRANT ALL ON TABLE stg.aircrafts_data TO postgres;


-- stg.airports_data definition

-- Drop table

-- DROP TABLE stg.airports_data;

CREATE TABLE stg.airports_data (
	airport_code bpchar(3) NOT NULL, -- Airport code
	airport_name jsonb NOT NULL, -- Airport name
	city jsonb NOT NULL, -- City
	coordinates point NOT NULL, -- Airport coordinates (longitude and latitude)
	timezone text NOT NULL, -- Airport time zone
	created_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	updated_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	CONSTRAINT airports_data_pkey PRIMARY KEY (airport_code)
);
COMMENT ON TABLE stg.airports_data IS 'Airports (internal data)';

-- Column comments

COMMENT ON COLUMN stg.airports_data.airport_code IS 'Airport code';
COMMENT ON COLUMN stg.airports_data.airport_name IS 'Airport name';
COMMENT ON COLUMN stg.airports_data.city IS 'City';
COMMENT ON COLUMN stg.airports_data.coordinates IS 'Airport coordinates (longitude and latitude)';
COMMENT ON COLUMN stg.airports_data.timezone IS 'Airport time zone';

-- Permissions

ALTER TABLE stg.airports_data OWNER TO postgres;
GRANT ALL ON TABLE stg.airports_data TO postgres;


-- stg.bookings definition

-- Drop table

-- DROP TABLE stg.bookings;

CREATE TABLE stg.bookings (
	book_ref bpchar(6) NOT NULL, -- Booking number
	book_date timestamptz NOT NULL, -- Booking date
	total_amount numeric(10, 2) NOT NULL, -- Total booking cost
	created_at timestamp NULL,
	updated_at timestamp NULL,
	CONSTRAINT bookings_pkey PRIMARY KEY (book_ref)
);
COMMENT ON TABLE stg.bookings IS 'Bookings';

-- Column comments

COMMENT ON COLUMN stg.bookings.book_ref IS 'Booking number';
COMMENT ON COLUMN stg.bookings.book_date IS 'Booking date';
COMMENT ON COLUMN stg.bookings.total_amount IS 'Total booking cost';

-- Permissions

ALTER TABLE stg.bookings OWNER TO postgres;
GRANT ALL ON TABLE stg.bookings TO postgres;


-- stg.flights definition

-- Drop table

-- DROP TABLE stg.flights;

CREATE TABLE stg.flights (
	flight_id serial4 NOT NULL, -- Flight ID
	flight_no bpchar(6) NOT NULL, -- Flight number
	scheduled_departure timestamptz NOT NULL, -- Scheduled departure time
	scheduled_arrival timestamptz NOT NULL, -- Scheduled arrival time
	departure_airport bpchar(3) NOT NULL, -- Airport of departure
	arrival_airport bpchar(3) NOT NULL, -- Airport of arrival
	status varchar(20) NOT NULL, -- Flight status
	aircraft_code bpchar(3) NOT NULL, -- Aircraft code, IATA
	actual_departure timestamptz NULL, -- Actual departure time
	actual_arrival timestamptz NULL, -- Actual arrival time
	created_at timestamp NULL,
	updated_at timestamp NULL,
	CONSTRAINT flights_check CHECK ((scheduled_arrival > scheduled_departure)),
	CONSTRAINT flights_check1 CHECK (((actual_arrival IS NULL) OR ((actual_departure IS NOT NULL) AND (actual_arrival IS NOT NULL) AND (actual_arrival > actual_departure)))),
	CONSTRAINT flights_flight_no_scheduled_departure_key UNIQUE (flight_no, scheduled_departure),
	CONSTRAINT flights_pkey PRIMARY KEY (flight_id),
	CONSTRAINT flights_status_check CHECK (((status)::text = ANY (ARRAY[('On Time'::character varying)::text, ('Delayed'::character varying)::text, ('Departed'::character varying)::text, ('Arrived'::character varying)::text, ('Scheduled'::character varying)::text, ('Cancelled'::character varying)::text]))),
	CONSTRAINT flights_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES stg.aircrafts_data(aircraft_code),
	CONSTRAINT flights_arrival_airport_fkey FOREIGN KEY (arrival_airport) REFERENCES stg.airports_data(airport_code),
	CONSTRAINT flights_departure_airport_fkey FOREIGN KEY (departure_airport) REFERENCES stg.airports_data(airport_code)
);
COMMENT ON TABLE stg.flights IS 'Flights';

-- Column comments

COMMENT ON COLUMN stg.flights.flight_id IS 'Flight ID';
COMMENT ON COLUMN stg.flights.flight_no IS 'Flight number';
COMMENT ON COLUMN stg.flights.scheduled_departure IS 'Scheduled departure time';
COMMENT ON COLUMN stg.flights.scheduled_arrival IS 'Scheduled arrival time';
COMMENT ON COLUMN stg.flights.departure_airport IS 'Airport of departure';
COMMENT ON COLUMN stg.flights.arrival_airport IS 'Airport of arrival';
COMMENT ON COLUMN stg.flights.status IS 'Flight status';
COMMENT ON COLUMN stg.flights.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.flights.actual_departure IS 'Actual departure time';
COMMENT ON COLUMN stg.flights.actual_arrival IS 'Actual arrival time';

-- Permissions

ALTER TABLE stg.flights OWNER TO postgres;
GRANT ALL ON TABLE stg.flights TO postgres;


-- stg.seats definition

-- Drop table

-- DROP TABLE stg.seats;

CREATE TABLE stg.seats (
	aircraft_code bpchar(3) NOT NULL, -- Aircraft code, IATA
	seat_no varchar(4) NOT NULL, -- Seat number
	fare_conditions varchar(10) NOT NULL, -- Travel class
	created_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	updated_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	CONSTRAINT seats_fare_conditions_check CHECK (((fare_conditions)::text = ANY (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text, ('Business'::character varying)::text]))),
	CONSTRAINT seats_pkey PRIMARY KEY (aircraft_code, seat_no),
	CONSTRAINT seats_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES stg.aircrafts_data(aircraft_code) ON DELETE CASCADE
);
COMMENT ON TABLE stg.seats IS 'Seats';

-- Column comments

COMMENT ON COLUMN stg.seats.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.seats.seat_no IS 'Seat number';
COMMENT ON COLUMN stg.seats.fare_conditions IS 'Travel class';

-- Permissions

ALTER TABLE stg.seats OWNER TO postgres;
GRANT ALL ON TABLE stg.seats TO postgres;


-- stg.tickets definition

-- Drop table

-- DROP TABLE stg.tickets;

CREATE TABLE stg.tickets (
	ticket_no bpchar(13) NOT NULL, -- Ticket number
	book_ref bpchar(6) NOT NULL, -- Booking number
	passenger_id varchar(20) NOT NULL, -- Passenger ID
	passenger_name text NOT NULL, -- Passenger name
	contact_data jsonb NULL, -- Passenger contact information
	created_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	updated_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	CONSTRAINT tickets_pkey PRIMARY KEY (ticket_no),
	CONSTRAINT tickets_book_ref_fkey FOREIGN KEY (book_ref) REFERENCES stg.bookings(book_ref)
);
COMMENT ON TABLE stg.tickets IS 'Tickets';

-- Column comments

COMMENT ON COLUMN stg.tickets.ticket_no IS 'Ticket number';
COMMENT ON COLUMN stg.tickets.book_ref IS 'Booking number';
COMMENT ON COLUMN stg.tickets.passenger_id IS 'Passenger ID';
COMMENT ON COLUMN stg.tickets.passenger_name IS 'Passenger name';
COMMENT ON COLUMN stg.tickets.contact_data IS 'Passenger contact information';

-- Permissions

ALTER TABLE stg.tickets OWNER TO postgres;
GRANT ALL ON TABLE stg.tickets TO postgres;


-- stg.ticket_flights definition

-- Drop table

-- DROP TABLE stg.ticket_flights;

CREATE TABLE stg.ticket_flights (
	ticket_no bpchar(13) NOT NULL, -- Ticket number
	flight_id int4 NOT NULL, -- Flight ID
	fare_conditions varchar(10) NOT NULL, -- Travel class
	amount numeric(10, 2) NOT NULL, -- Travel cost
	created_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	updated_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	CONSTRAINT ticket_flights_amount_check CHECK ((amount >= (0)::numeric)),
	CONSTRAINT ticket_flights_fare_conditions_check CHECK (((fare_conditions)::text = ANY (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text, ('Business'::character varying)::text]))),
	CONSTRAINT ticket_flights_pkey PRIMARY KEY (ticket_no, flight_id),
	CONSTRAINT ticket_flights_flight_id_fkey FOREIGN KEY (flight_id) REFERENCES stg.flights(flight_id),
	CONSTRAINT ticket_flights_ticket_no_fkey FOREIGN KEY (ticket_no) REFERENCES stg.tickets(ticket_no)
);
COMMENT ON TABLE stg.ticket_flights IS 'Flight segment';

-- Column comments

COMMENT ON COLUMN stg.ticket_flights.ticket_no IS 'Ticket number';
COMMENT ON COLUMN stg.ticket_flights.flight_id IS 'Flight ID';
COMMENT ON COLUMN stg.ticket_flights.fare_conditions IS 'Travel class';
COMMENT ON COLUMN stg.ticket_flights.amount IS 'Travel cost';

-- Permissions

ALTER TABLE stg.ticket_flights OWNER TO postgres;
GRANT ALL ON TABLE stg.ticket_flights TO postgres;


-- stg.boarding_passes definition

-- Drop table

-- DROP TABLE stg.boarding_passes;

CREATE TABLE stg.boarding_passes (
	ticket_no bpchar(13) NOT NULL, -- Ticket number
	flight_id int4 NOT NULL, -- Flight ID
	boarding_no int4 NOT NULL, -- Boarding pass number
	seat_no varchar(4) NOT NULL, -- Seat number
	created_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	updated_at timestamp DEFAULT '2017-01-01 07:00:00'::timestamp without time zone NULL,
	CONSTRAINT boarding_passes_flight_id_boarding_no_key UNIQUE (flight_id, boarding_no),
	CONSTRAINT boarding_passes_flight_id_seat_no_key UNIQUE (flight_id, seat_no),
	CONSTRAINT boarding_passes_pkey PRIMARY KEY (ticket_no, flight_id),
	CONSTRAINT boarding_passes_ticket_no_fkey FOREIGN KEY (ticket_no,flight_id) REFERENCES stg.ticket_flights(ticket_no,flight_id)
);
COMMENT ON TABLE stg.boarding_passes IS 'Boarding passes';

-- Column comments

COMMENT ON COLUMN stg.boarding_passes.ticket_no IS 'Ticket number';
COMMENT ON COLUMN stg.boarding_passes.flight_id IS 'Flight ID';
COMMENT ON COLUMN stg.boarding_passes.boarding_no IS 'Boarding pass number';
COMMENT ON COLUMN stg.boarding_passes.seat_no IS 'Seat number';

-- Permissions

ALTER TABLE stg.boarding_passes OWNER TO postgres;
GRANT ALL ON TABLE stg.boarding_passes TO postgres;


-- stg.aircrafts source

CREATE OR REPLACE VIEW stg.aircrafts
AS SELECT aircraft_code,
    model ->> bookings.lang() AS model,
    range
   FROM bookings.aircrafts_data ml;

COMMENT ON VIEW stg.aircrafts IS 'Aircrafts';
COMMENT ON COLUMN stg.aircrafts.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.aircrafts.model IS 'Aircraft model';
COMMENT ON COLUMN stg.aircrafts."range" IS 'Maximal flying distance, km';

-- Permissions

ALTER TABLE stg.aircrafts OWNER TO postgres;
GRANT ALL ON TABLE stg.aircrafts TO postgres;


-- stg.airports source

CREATE OR REPLACE VIEW stg.airports
AS SELECT airport_code,
    airport_name ->> bookings.lang() AS airport_name,
    city ->> bookings.lang() AS city,
    coordinates,
    timezone
   FROM bookings.airports_data ml;

COMMENT ON VIEW stg.airports IS 'Airports';
COMMENT ON COLUMN stg.airports.airport_code IS 'Airport code';
COMMENT ON COLUMN stg.airports.airport_name IS 'Airport name';
COMMENT ON COLUMN stg.airports.city IS 'City';
COMMENT ON COLUMN stg.airports.coordinates IS 'Airport coordinates (longitude and latitude)';
COMMENT ON COLUMN stg.airports.timezone IS 'Airport time zone';

-- Permissions

ALTER TABLE stg.airports OWNER TO postgres;
GRANT ALL ON TABLE stg.airports TO postgres;


-- stg.flights_v source

CREATE OR REPLACE VIEW stg.flights_v
AS SELECT f.flight_id,
    f.flight_no,
    f.scheduled_departure,
    timezone(dep.timezone, f.scheduled_departure) AS scheduled_departure_local,
    f.scheduled_arrival,
    timezone(arr.timezone, f.scheduled_arrival) AS scheduled_arrival_local,
    f.scheduled_arrival - f.scheduled_departure AS scheduled_duration,
    f.departure_airport,
    dep.airport_name AS departure_airport_name,
    dep.city AS departure_city,
    f.arrival_airport,
    arr.airport_name AS arrival_airport_name,
    arr.city AS arrival_city,
    f.status,
    f.aircraft_code,
    f.actual_departure,
    timezone(dep.timezone, f.actual_departure) AS actual_departure_local,
    f.actual_arrival,
    timezone(arr.timezone, f.actual_arrival) AS actual_arrival_local,
    f.actual_arrival - f.actual_departure AS actual_duration
   FROM bookings.flights f,
    bookings.airports dep,
    bookings.airports arr
  WHERE f.departure_airport = dep.airport_code AND f.arrival_airport = arr.airport_code;

COMMENT ON VIEW stg.flights_v IS 'Flights (extended)';
COMMENT ON COLUMN stg.flights_v.flight_id IS 'Flight ID';
COMMENT ON COLUMN stg.flights_v.flight_no IS 'Flight number';
COMMENT ON COLUMN stg.flights_v.scheduled_departure IS 'Scheduled departure time';
COMMENT ON COLUMN stg.flights_v.scheduled_departure_local IS 'Scheduled departure time, local time at the point of departure';
COMMENT ON COLUMN stg.flights_v.scheduled_arrival IS 'Scheduled arrival time';
COMMENT ON COLUMN stg.flights_v.scheduled_arrival_local IS 'Scheduled arrival time, local time at the point of destination';
COMMENT ON COLUMN stg.flights_v.scheduled_duration IS 'Scheduled flight duration';
COMMENT ON COLUMN stg.flights_v.departure_airport IS 'Deprature airport code';
COMMENT ON COLUMN stg.flights_v.departure_airport_name IS 'Departure airport name';
COMMENT ON COLUMN stg.flights_v.departure_city IS 'City of departure';
COMMENT ON COLUMN stg.flights_v.arrival_airport IS 'Arrival airport code';
COMMENT ON COLUMN stg.flights_v.arrival_airport_name IS 'Arrival airport name';
COMMENT ON COLUMN stg.flights_v.arrival_city IS 'City of arrival';
COMMENT ON COLUMN stg.flights_v.status IS 'Flight status';
COMMENT ON COLUMN stg.flights_v.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.flights_v.actual_departure IS 'Actual departure time';
COMMENT ON COLUMN stg.flights_v.actual_departure_local IS 'Actual departure time, local time at the point of departure';
COMMENT ON COLUMN stg.flights_v.actual_arrival IS 'Actual arrival time';
COMMENT ON COLUMN stg.flights_v.actual_arrival_local IS 'Actual arrival time, local time at the point of destination';
COMMENT ON COLUMN stg.flights_v.actual_duration IS 'Actual flight duration';

-- Permissions

ALTER TABLE stg.flights_v OWNER TO postgres;
GRANT ALL ON TABLE stg.flights_v TO postgres;


-- stg.routes source

CREATE OR REPLACE VIEW stg.routes
AS WITH f3 AS (
         SELECT f2.flight_no,
            f2.departure_airport,
            f2.arrival_airport,
            f2.aircraft_code,
            f2.duration,
            array_agg(f2.days_of_week) AS days_of_week
           FROM ( SELECT f1.flight_no,
                    f1.departure_airport,
                    f1.arrival_airport,
                    f1.aircraft_code,
                    f1.duration,
                    f1.days_of_week
                   FROM ( SELECT flights.flight_no,
                            flights.departure_airport,
                            flights.arrival_airport,
                            flights.aircraft_code,
                            flights.scheduled_arrival - flights.scheduled_departure AS duration,
                            to_char(flights.scheduled_departure, 'ID'::text)::integer AS days_of_week
                           FROM bookings.flights) f1
                  GROUP BY f1.flight_no, f1.departure_airport, f1.arrival_airport, f1.aircraft_code, f1.duration, f1.days_of_week
                  ORDER BY f1.flight_no, f1.departure_airport, f1.arrival_airport, f1.aircraft_code, f1.duration, f1.days_of_week) f2
          GROUP BY f2.flight_no, f2.departure_airport, f2.arrival_airport, f2.aircraft_code, f2.duration
        )
 SELECT f3.flight_no,
    f3.departure_airport,
    dep.airport_name AS departure_airport_name,
    dep.city AS departure_city,
    f3.arrival_airport,
    arr.airport_name AS arrival_airport_name,
    arr.city AS arrival_city,
    f3.aircraft_code,
    f3.duration,
    f3.days_of_week
   FROM f3,
    bookings.airports dep,
    bookings.airports arr
  WHERE f3.departure_airport = dep.airport_code AND f3.arrival_airport = arr.airport_code;

COMMENT ON VIEW stg.routes IS 'Routes';
COMMENT ON COLUMN stg.routes.flight_no IS 'Flight number';
COMMENT ON COLUMN stg.routes.departure_airport IS 'Code of airport of departure';
COMMENT ON COLUMN stg.routes.departure_airport_name IS 'Name of airport of departure';
COMMENT ON COLUMN stg.routes.departure_city IS 'City of departure';
COMMENT ON COLUMN stg.routes.arrival_airport IS 'Code of airport of arrival';
COMMENT ON COLUMN stg.routes.arrival_airport_name IS 'Name of airport of arrival';
COMMENT ON COLUMN stg.routes.arrival_city IS 'City of arrival';
COMMENT ON COLUMN stg.routes.aircraft_code IS 'Aircraft code, IATA';
COMMENT ON COLUMN stg.routes.duration IS 'Scheduled duration of flight';
COMMENT ON COLUMN stg.routes.days_of_week IS 'Days of week on which flights are scheduled';

-- Permissions

ALTER TABLE stg.routes OWNER TO postgres;
GRANT ALL ON TABLE stg.routes TO postgres;



-- DROP FUNCTION stg.lang();

CREATE OR REPLACE FUNCTION bookings.lang()
 RETURNS text
 LANGUAGE plpgsql
 STABLE
AS $function$
BEGIN
  RETURN current_setting('bookings.lang');
EXCEPTION
  WHEN undefined_object THEN
    RETURN NULL;
END;
$function$
;

-- Permissions

ALTER FUNCTION stg.lang() OWNER TO postgres;
GRANT ALL ON FUNCTION stg.lang() TO postgres;

-- DROP FUNCTION stg.now();

CREATE OR REPLACE FUNCTION bookings.now()
 RETURNS timestamp with time zone
 LANGUAGE sql
 IMMUTABLE
AS $function$SELECT '2017-08-15 18:00:00'::TIMESTAMP AT TIME ZONE 'Europe/Moscow';$function$
;

COMMENT ON FUNCTION stg.now() IS 'Point in time according to which the data are generated';

-- Permissions

ALTER FUNCTION stg.now() OWNER TO postgres;
GRANT ALL ON FUNCTION stg.now() TO postgres;


-- Permissions

GRANT ALL ON SCHEMA stg TO postgres;


--------------------------------------------------------------------------------------------------------------------------------- FINAL SCHEMA
-- time dimension
DROP TABLE if exists prod.dim_time;
CREATE TABLE prod.dim_time
(
	time_id integer NOT NULL,
	time_actual time NOT NULL,
	hours_24 character(2) NOT NULL,
	hours_12 character(2) NOT NULL,
	hour_minutes character (2)  NOT NULL,
	day_minutes integer NOT NULL,
	day_time_name character varying (20) NOT NULL,
	day_night character varying (20) NOT NULL,
	CONSTRAINT time_pk PRIMARY KEY (time_id)
);

DROP TABLE if exists prod.dim_date;
CREATE TABLE prod.dim_date
(
  date_id              INT NOT null primary KEY,
  date_actual              DATE NOT NULL,
  day_suffix               VARCHAR(4) NOT NULL,
  day_name                 VARCHAR(9) NOT NULL,
  day_of_year              INT NOT NULL,
  week_of_month            INT NOT NULL,
  week_of_year             INT NOT NULL,
  week_of_year_iso         CHAR(10) NOT NULL,
  month_actual             INT NOT NULL,
  month_name               VARCHAR(9) NOT NULL,
  month_name_abbreviated   CHAR(3) NOT NULL,
  quarter_actual           INT NOT NULL,
  quarter_name             VARCHAR(9) NOT NULL,
  year_actual              INT NOT NULL,
  first_day_of_week        DATE NOT NULL,
  last_day_of_week         DATE NOT NULL,
  first_day_of_month       DATE NOT NULL,
  last_day_of_month        DATE NOT NULL,
  first_day_of_quarter     DATE NOT NULL,
  last_day_of_quarter      DATE NOT NULL,
  first_day_of_year        DATE NOT NULL,
  last_day_of_year         DATE NOT NULL,
  mmyyyy                   CHAR(6) NOT NULL,
  mmddyyyy                 CHAR(10) NOT NULL,
  weekend_indr             VARCHAR(20) NOT NULL
);

CREATE INDEX dim_date_date_actual_idx
  ON prod.dim_date(date_actual);


-- dim passenger
CREATE TABLE prod.dim_passenger (
    passenger_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    passenger_nk VARCHAR(20) NOT NULL,
    passenger_name VARCHAR(255),
    phone VARCHAR(30),
    email VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE prod.dim_aircraft (
    aircraft_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_nk BPCHAR(3) NOT NULL,
    model VARCHAR(255),
    "range" INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- dim_airport
CREATE TABLE prod.dim_airport (
    airport_id UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    airport_nk BPCHAR(3) NOT NULL,
    airport_name VARCHAR(255),
    city VARCHAR(255),
    coordinates POINT,
    timezone TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE prod.dim_seat (
    seat_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    aircraft_id UUID,
    seat_no VARCHAR(4) NOT NULL,
    fare_conditions VARCHAR(10),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_aircraft_seat FOREIGN KEY (aircraft_id) REFERENCES prod.dim_aircraft(aircraft_id)
);

CREATE TABLE prod.fct_booking_ticket (
    booking_ticket_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    book_nk BPCHAR(6),
    ticket_no BPCHAR(13),
    passenger_id UUID,
    flight_nk INT4,
    flight_no VARCHAR,
    -- Foreign Keys
    book_date_local INT,
    book_date_utc INT,
    book_time_local INT,
    book_time_utc INT,
    scheduled_departure_date_local INT,
    scheduled_departure_date_utc INT,
    scheduled_departure_time_local INT,
    scheduled_departure_time_utc INT,
    scheduled_arrival_date_local INT,
    scheduled_arrival_date_utc INT,
    scheduled_arrival_time_local INT,
    scheduled_arrival_time_utc INT,
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    actual_departure_date_local INT,
    actual_departure_date_utc INT,
    actual_departure_time_local INT,
    actual_departure_time_utc INT,
    actual_arrival_date_local INT,
    actual_arrival_date_utc INT,
    actual_arrival_time_local INT,
    actual_arrival_time_utc INT,
    fare_conditions VARCHAR(10),
    amount NUMERIC(10, 2),
    total_amount NUMERIC(10, 2),
    status VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_booking_passenger_id FOREIGN KEY (passenger_id) REFERENCES prod.dim_passenger(passenger_id),
    CONSTRAINT fk_book_date_local FOREIGN KEY (book_date_local) REFERENCES prod.dim_date,
    CONSTRAINT fk_book_date_utc FOREIGN KEY (book_date_utc) REFERENCES prod.dim_date,
    CONSTRAINT fk_book_time_local FOREIGN KEY (book_time_local) REFERENCES prod.dim_time,
    CONSTRAINT fk_book_time_utc FOREIGN KEY (book_time_utc) REFERENCES prod.dim_time,
    CONSTRAINT fk_scheduled_departure_date_local FOREIGN KEY (scheduled_departure_date_local) REFERENCES prod.dim_date,
    CONSTRAINT fk_scheduled_departure_date_utc FOREIGN KEY (scheduled_departure_date_utc) REFERENCES prod.dim_date,
    CONSTRAINT fk_scheduled_departure_time_local FOREIGN KEY (scheduled_departure_time_local) REFERENCES prod.dim_time,
    CONSTRAINT fk_scheduled_departure_time_utc FOREIGN KEY (scheduled_departure_time_utc) REFERENCES prod.dim_time,
    CONSTRAINT fk_scheduled_arrival_date_local FOREIGN KEY (scheduled_arrival_date_local) REFERENCES prod.dim_date,
    CONSTRAINT fk_scheduled_arrival_date_utc FOREIGN KEY (scheduled_arrival_date_utc) REFERENCES prod.dim_date,
    CONSTRAINT fk_scheduled_arrival_time_local FOREIGN KEY (scheduled_arrival_time_local) REFERENCES prod.dim_time,
    CONSTRAINT fk_scheduled_arrival_time_utc FOREIGN KEY (scheduled_arrival_time_utc) REFERENCES prod.dim_time,
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES prod.dim_aircraft(aircraft_id),
    CONSTRAINT fk_actual_departure_date_local FOREIGN KEY (actual_departure_date_local) REFERENCES prod.dim_date,
    CONSTRAINT fk_actual_departure_date_utc FOREIGN KEY (actual_departure_date_utc) REFERENCES prod.dim_date,
    CONSTRAINT fk_actual_departure_time_local FOREIGN KEY (actual_departure_time_local) REFERENCES prod.dim_time,
    CONSTRAINT fk_actual_departure_time_utc FOREIGN KEY (actual_departure_time_utc) REFERENCES prod.dim_time,
    CONSTRAINT fk_actual_arrival_date_local FOREIGN KEY (actual_arrival_date_local) REFERENCES prod.dim_date,
    CONSTRAINT fk_actual_arrival_date_utc FOREIGN KEY (actual_arrival_date_utc) REFERENCES prod.dim_date,
    CONSTRAINT fk_actual_arrival_time_local FOREIGN KEY (actual_arrival_time_local) REFERENCES prod.dim_time,
    CONSTRAINT fk_actual_arrival_time_utc FOREIGN KEY (actual_arrival_time_utc) REFERENCES prod.dim_time
);


CREATE TABLE prod.fct_flight_activity (
    flight_activity_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    flight_nk BPCHAR(6),
    flight_no VARCHAR,
    -- Foreign Keys
    scheduled_departure_date_local INT,
    scheduled_departure_date_utc INT,
    scheduled_departure_time_local INT,
    scheduled_departure_time_utc INT,
    scheduled_arrival_date_local INT,
    scheduled_arrival_date_utc INT,
    scheduled_arrival_time_local INT,
    scheduled_arrival_time_utc INT,
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    actual_departure_date_local INT,
    actual_departure_date_utc INT,
    actual_departure_time_local INT,
    actual_departure_time_utc INT,
    actual_arrival_date_local INT,
    actual_arrival_date_utc INT,
    actual_arrival_time_local INT,
    actual_arrival_time_utc INT,
    status VARCHAR(20),
    delay_departure INTERVAL,
    delay_arrival INTERVAL,
    travel_time INTERVAL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_scheduled_departure_date_local FOREIGN KEY (scheduled_departure_date_local) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_date_utc FOREIGN KEY (scheduled_departure_date_utc) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_time_local FOREIGN KEY (scheduled_departure_time_local) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_scheduled_departure_time_utc FOREIGN KEY (scheduled_departure_time_utc) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_date_local FOREIGN KEY (scheduled_arrival_date_local) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_date_utc FOREIGN KEY (scheduled_arrival_date_utc) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_time_local FOREIGN KEY (scheduled_arrival_time_local) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_time_utc FOREIGN KEY (scheduled_arrival_time_utc) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES prod.dim_aircraft(aircraft_id),
    CONSTRAINT fk_actual_departure_date_local FOREIGN KEY (actual_departure_date_local) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_actual_departure_date_utc FOREIGN KEY (actual_departure_date_utc) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_actual_departure_time_local FOREIGN KEY (actual_departure_time_local) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_actual_departure_time_utc FOREIGN KEY (actual_departure_time_utc) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_actual_arrival_date_local FOREIGN KEY (actual_arrival_date_local) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_actual_arrival_date_utc FOREIGN KEY (actual_arrival_date_utc) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_actual_arrival_time_local FOREIGN KEY (actual_arrival_time_local) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_actual_arrival_time_utc FOREIGN KEY (actual_arrival_time_utc) REFERENCES prod.dim_time(time_id)
);

CREATE TABLE prod.fct_seat_occupied_daily (
    seat_occupied_daily_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    date_flight INT,
    flight_nk BPCHAR(6),
    flight_no VARCHAR,
    -- Foreign Keys
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    status VARCHAR(20),
    total_seat NUMERIC,
    seat_occupied NUMERIC,
    empty_seats NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_date_flight FOREIGN KEY (date_flight) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES prod.dim_aircraft(aircraft_id)
);

CREATE TABLE prod.fct_boarding_pass (
    boarding_pass_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticket_no BPCHAR(13),
    book_ref BPCHAR(6),
    passenger_id UUID,
    flight_id INT,
    flight_no VARCHAR,
    boarding_no INT,
    -- Foreign Keys
    scheduled_departure_date_local INT,
    scheduled_departure_date_utc INT,
    scheduled_departure_time_local INT,
    scheduled_departure_time_utc INT,
    scheduled_arrival_date_local INT,
    scheduled_arrival_date_utc INT,
    scheduled_arrival_time_local INT,
    scheduled_arrival_time_utc INT,
    departure_airport UUID,
    arrival_airport UUID,
    aircraft_code UUID,
    status VARCHAR(20),
    fare_conditions VARCHAR(10),
    seat_no VARCHAR(4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Constraints
    CONSTRAINT fk_passenger_id FOREIGN KEY (passenger_id) REFERENCES prod.dim_passenger(passenger_id),
    CONSTRAINT fk_scheduled_departure_date_local FOREIGN KEY (scheduled_departure_date_local) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_date_utc FOREIGN KEY (scheduled_departure_date_utc) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_departure_time_local FOREIGN KEY (scheduled_departure_time_local) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_scheduled_departure_time_utc FOREIGN KEY (scheduled_departure_time_utc) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_date_local FOREIGN KEY (scheduled_arrival_date_local) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_date_utc FOREIGN KEY (scheduled_arrival_date_utc) REFERENCES prod.dim_date(date_id),
    CONSTRAINT fk_scheduled_arrival_time_local FOREIGN KEY (scheduled_arrival_time_local) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_scheduled_arrival_time_utc FOREIGN KEY (scheduled_arrival_time_utc) REFERENCES prod.dim_time(time_id),
    CONSTRAINT fk_departure_airport FOREIGN KEY (departure_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_arrival_airport FOREIGN KEY (arrival_airport) REFERENCES prod.dim_airport(airport_id),
    CONSTRAINT fk_aircraft_code FOREIGN KEY (aircraft_code) REFERENCES prod.dim_aircraft(aircraft_id)
);

INSERT INTO prod.dim_date
SELECT TO_CHAR(datum, 'yyyymmdd')::INT AS date_id,
       datum AS date_actual,
       TO_CHAR(datum, 'fmDDth') AS day_suffix,
       TO_CHAR(datum, 'TMDay') AS day_name,
       EXTRACT(DOY FROM datum) AS day_of_year,
       TO_CHAR(datum, 'W')::INT AS week_of_month,
       EXTRACT(WEEK FROM datum) AS week_of_year,
       EXTRACT(ISOYEAR FROM datum) || TO_CHAR(datum, '"-W"IW') AS week_of_year_iso,
       EXTRACT(MONTH FROM datum) AS month_actual,
       TO_CHAR(datum, 'TMMonth') AS month_name,
       TO_CHAR(datum, 'Mon') AS month_name_abbreviated,
       EXTRACT(QUARTER FROM datum) AS quarter_actual,
       CASE
           WHEN EXTRACT(QUARTER FROM datum) = 1 THEN 'First'
           WHEN EXTRACT(QUARTER FROM datum) = 2 THEN 'Second'
           WHEN EXTRACT(QUARTER FROM datum) = 3 THEN 'Third'
           WHEN EXTRACT(QUARTER FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
       EXTRACT(YEAR FROM datum) AS year_actual,
       datum + (1 - EXTRACT(ISODOW FROM datum))::INT AS first_day_of_week,
       datum + (7 - EXTRACT(ISODOW FROM datum))::INT AS last_day_of_week,
       datum + (1 - EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
       (DATE_TRUNC('MONTH', datum) + INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
       DATE_TRUNC('quarter', datum)::DATE AS first_day_of_quarter,
       (DATE_TRUNC('quarter', datum) + INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-01-01', 'YYYY-MM-DD') AS first_day_of_year,
       TO_DATE(EXTRACT(YEAR FROM datum) || '-12-31', 'YYYY-MM-DD') AS last_day_of_year,
       TO_CHAR(datum, 'mmyyyy') AS mmyyyy,
       TO_CHAR(datum, 'mmddyyyy') AS mmddyyyy,
       CASE
           WHEN EXTRACT(ISODOW FROM datum) IN (6, 7) THEN 'weekend'
           ELSE 'weekday'
           END AS weekend_indr
FROM (SELECT '1998-01-01'::DATE + SEQUENCE.DAY AS datum
      FROM GENERATE_SERIES(0, 29219) AS SEQUENCE (DAY)
      GROUP BY SEQUENCE.DAY) DQ
ORDER BY 1;

-- populate time dimension
insert into  prod.dim_time

SELECT  
	cast(to_char(minute, 'hh24mi') as numeric) time_id,
	to_char(minute, 'hh24:mi')::time AS tume_actual,
	-- Hour of the day (0 - 23)
	to_char(minute, 'hh24') AS hour_24,
	-- Hour of the day (0 - 11)
	to_char(minute, 'hh12') hour_12,
	-- Hour minute (0 - 59)
	to_char(minute, 'mi') hour_minutes,
	-- Minute of the day (0 - 1439)
	extract(hour FROM minute)*60 + extract(minute FROM minute) day_minutes,
	-- Names of day periods
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '00:00' AND '11:59'
		then 'AM'
		when to_char(minute, 'hh24:mi') BETWEEN '12:00' AND '23:59'
		then 'PM'
	end AS day_time_name,
	-- Indicator of day or night
	case 
		when to_char(minute, 'hh24:mi') BETWEEN '07:00' AND '19:59' then 'Day'	
		else 'Night'
	end AS day_night
FROM 
	(SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute 
	FROM  generate_series(0,1439) AS sequence(minute)
GROUP BY sequence.minute
) DQ
ORDER BY 1;
