CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- CREATE SCHEMA FOR STAGING & FINAL(production)
CREATE SCHEMA IF NOT EXISTS stg AUTHORIZATION postgres;
CREATE SCHEMA IF NOT EXISTS prod AUTHORIZATION postgres;

------------------------------------------------------------------------------------------------------------------------------ STAGING SCHEMA
--
-- Name: aircrafts_data; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.aircrafts_data (
    aircraft_code character(3) NOT NULL,
    model jsonb NOT NULL,
    range integer NOT NULL,
    created_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    updated_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    CONSTRAINT aircrafts_range_check CHECK ((range > 0))
);


ALTER TABLE stg.aircrafts_data OWNER TO postgres;

--
-- Name: TABLE aircrafts_data; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON TABLE stg.aircrafts_data IS 'Aircrafts (internal data)';


--
-- Name: COLUMN aircrafts_data.aircraft_code; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.aircrafts_data.aircraft_code IS 'Aircraft code, IATA';


--
-- Name: COLUMN aircrafts_data.model; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.aircrafts_data.model IS 'Aircraft model';


--
-- Name: COLUMN aircrafts_data.range; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.aircrafts_data.range IS 'Maximal flying distance, km';


--
-- Name: airports_data; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.airports_data (
    airport_code character(3) NOT NULL,
    airport_name jsonb NOT NULL,
    city jsonb NOT NULL,
    coordinates point NOT NULL,
    timezone text NOT NULL,
    created_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    updated_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone
);


ALTER TABLE stg.airports_data OWNER TO postgres;

--
-- Name: TABLE airports_data; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON TABLE stg.airports_data IS 'Airports (internal data)';


--
-- Name: COLUMN airports_data.airport_code; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.airports_data.airport_code IS 'Airport code';


--
-- Name: COLUMN airports_data.airport_name; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.airports_data.airport_name IS 'Airport name';


--
-- Name: COLUMN airports_data.city; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.airports_data.city IS 'City';


--
-- Name: COLUMN airports_data.coordinates; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.airports_data.coordinates IS 'Airport coordinates (longitude and latitude)';


--
-- Name: COLUMN airports_data.timezone; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.airports_data.timezone IS 'Airport time zone';

--
-- Name: boarding_passes; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.boarding_passes (
    ticket_no character(13) NOT NULL,
    flight_id integer NOT NULL,
    boarding_no integer NOT NULL,
    seat_no character varying(4) NOT NULL,
    created_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    updated_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone
);


ALTER TABLE stg.boarding_passes OWNER TO postgres;

--
-- Name: TABLE boarding_passes; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON TABLE stg.boarding_passes IS 'Boarding passes';


--
-- Name: COLUMN boarding_passes.ticket_no; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.boarding_passes.ticket_no IS 'Ticket number';


--
-- Name: COLUMN boarding_passes.flight_id; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.boarding_passes.flight_id IS 'Flight ID';


--
-- Name: COLUMN boarding_passes.boarding_no; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.boarding_passes.boarding_no IS 'Boarding pass number';


--
-- Name: COLUMN boarding_passes.seat_no; Type: COMMENT; Schema: stg; Owner: postgres
--

COMMENT ON COLUMN stg.boarding_passes.seat_no IS 'Seat number';


--
-- Name: bookings; Type: TABLE; Schema: stg; Owner: postgres
--

CREATE TABLE stg.bookings (
    book_ref character(6) NOT NULL,
    book_date timestamp with time zone NOT NULL,
    total_amount numeric(10,2) NOT NULL,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);


ALTER TABLE stg.bookings OWNER TO postgres;

--
-- Name: TABLE bookings; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON TABLE stg.bookings IS 'Bookings';


--
-- Name: COLUMN bookings.book_ref; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.bookings.book_ref IS 'Booking number';


--
-- Name: COLUMN bookings.book_date; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.bookings.book_date IS 'Booking date';


--
-- Name: COLUMN bookings.total_amount; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.bookings.total_amount IS 'Total booking cost';


--
-- Name: flights; Type: TABLE; Schema: bookings; Owner: postgres
--

CREATE TABLE stg.flights (
    flight_id integer NOT NULL,
    flight_no character(6) NOT NULL,
    scheduled_departure timestamp with time zone NOT NULL,
    scheduled_arrival timestamp with time zone NOT NULL,
    departure_airport character(3) NOT NULL,
    arrival_airport character(3) NOT NULL,
    status character varying(20) NOT NULL,
    aircraft_code character(3) NOT NULL,
    actual_departure timestamp with time zone,
    actual_arrival timestamp with time zone,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    CONSTRAINT flights_check CHECK ((scheduled_arrival > scheduled_departure)),
    CONSTRAINT flights_check1 CHECK (((actual_arrival IS NULL) OR ((actual_departure IS NOT NULL) AND (actual_arrival IS NOT NULL) AND (actual_arrival > actual_departure)))),
    CONSTRAINT flights_status_check CHECK (((status)::text = ANY (ARRAY[('On Time'::character varying)::text, ('Delayed'::character varying)::text, ('Departed'::character varying)::text, ('Arrived'::character varying)::text, ('Scheduled'::character varying)::text, ('Cancelled'::character varying)::text])))
);


ALTER TABLE stg.flights OWNER TO postgres;

--
-- Name: TABLE flights; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON TABLE stg.flights IS 'Flights';


--
-- Name: COLUMN flights.flight_id; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.flight_id IS 'Flight ID';


--
-- Name: COLUMN flights.flight_no; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.flight_no IS 'Flight number';


--
-- Name: COLUMN flights.scheduled_departure; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.scheduled_departure IS 'Scheduled departure time';


--
-- Name: COLUMN flights.scheduled_arrival; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.scheduled_arrival IS 'Scheduled arrival time';


--
-- Name: COLUMN flights.departure_airport; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.departure_airport IS 'Airport of departure';


--
-- Name: COLUMN flights.arrival_airport; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.arrival_airport IS 'Airport of arrival';


--
-- Name: COLUMN flights.status; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.status IS 'Flight status';


--
-- Name: COLUMN flights.aircraft_code; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.aircraft_code IS 'Aircraft code, IATA';


--
-- Name: COLUMN flights.actual_departure; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.actual_departure IS 'Actual departure time';


--
-- Name: COLUMN flights.actual_arrival; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.flights.actual_arrival IS 'Actual arrival time';


--
-- Name: flights_flight_id_seq; Type: SEQUENCE; Schema: bookings; Owner: postgres
--

CREATE SEQUENCE stg.flights_flight_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


ALTER SEQUENCE stg.flights_flight_id_seq OWNER TO postgres;

--
-- Name: flights_flight_id_seq; Type: SEQUENCE OWNED BY; Schema: bookings; Owner: postgres
--

ALTER SEQUENCE stg.flights_flight_id_seq OWNED BY stg.flights.flight_id;


--
-- Name: seats; Type: TABLE; Schema: bookings; Owner: postgres
--

CREATE TABLE stg.seats (
    aircraft_code character(3) NOT NULL,
    seat_no character varying(4) NOT NULL,
    fare_conditions character varying(10) NOT NULL,
    created_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    updated_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    CONSTRAINT seats_fare_conditions_check CHECK (((fare_conditions)::text = ANY (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text, ('Business'::character varying)::text])))
);


ALTER TABLE stg.seats OWNER TO postgres;

--
-- Name: TABLE seats; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON TABLE stg.seats IS 'Seats';


--
-- Name: COLUMN seats.aircraft_code; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.seats.aircraft_code IS 'Aircraft code, IATA';


--
-- Name: COLUMN seats.seat_no; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.seats.seat_no IS 'Seat number';


--
-- Name: COLUMN seats.fare_conditions; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.seats.fare_conditions IS 'Travel class';


--
-- Name: ticket_flights; Type: TABLE; Schema: bookings; Owner: postgres
--

CREATE TABLE stg.ticket_flights (
    ticket_no character(13) NOT NULL,
    flight_id integer NOT NULL,
    fare_conditions character varying(10) NOT NULL,
    amount numeric(10,2) NOT NULL,
    created_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    updated_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    CONSTRAINT ticket_flights_amount_check CHECK ((amount >= (0)::numeric)),
    CONSTRAINT ticket_flights_fare_conditions_check CHECK (((fare_conditions)::text = ANY (ARRAY[('Economy'::character varying)::text, ('Comfort'::character varying)::text, ('Business'::character varying)::text])))
);


ALTER TABLE stg.ticket_flights OWNER TO postgres;

--
-- Name: TABLE ticket_flights; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON TABLE stg.ticket_flights IS 'Flight segment';


--
-- Name: COLUMN ticket_flights.ticket_no; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.ticket_flights.ticket_no IS 'Ticket number';


--
-- Name: COLUMN ticket_flights.flight_id; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.ticket_flights.flight_id IS 'Flight ID';


--
-- Name: COLUMN ticket_flights.fare_conditions; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.ticket_flights.fare_conditions IS 'Travel class';


--
-- Name: COLUMN ticket_flights.amount; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.ticket_flights.amount IS 'Travel cost';


--
-- Name: tickets; Type: TABLE; Schema: bookings; Owner: postgres
--

CREATE TABLE stg.tickets (
    ticket_no character(13) NOT NULL,
    book_ref character(6) NOT NULL,
    passenger_id character varying(20) NOT NULL,
    passenger_name text NOT NULL,
    contact_data jsonb,
    created_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone,
    updated_at timestamp without time zone DEFAULT '2017-01-01 07:00:00'::timestamp without time zone
);


ALTER TABLE stg.tickets OWNER TO postgres;

--
-- Name: TABLE tickets; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON TABLE stg.tickets IS 'Tickets';


--
-- Name: COLUMN tickets.ticket_no; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.tickets.ticket_no IS 'Ticket number';


--
-- Name: COLUMN tickets.book_ref; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.tickets.book_ref IS 'Booking number';


--
-- Name: COLUMN tickets.passenger_id; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.tickets.passenger_id IS 'Passenger ID';


--
-- Name: COLUMN tickets.passenger_name; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.tickets.passenger_name IS 'Passenger name';


--
-- Name: COLUMN tickets.contact_data; Type: COMMENT; Schema: bookings; Owner: postgres
--

COMMENT ON COLUMN stg.tickets.contact_data IS 'Passenger contact information';


--
-- Name: flights flight_id; Type: DEFAULT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.flights ALTER COLUMN flight_id SET DEFAULT nextval('stg.flights_flight_id_seq'::regclass);


--
-- Name: flights_flight_id_seq; Type: SEQUENCE SET; Schema: bookings; Owner: postgres
--

SELECT pg_catalog.setval('stg.flights_flight_id_seq', 33121, true);


--
-- Name: aircrafts_data aircrafts_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.aircrafts_data
    ADD CONSTRAINT aircrafts_pkey PRIMARY KEY (aircraft_code);


--
-- Name: airports_data airports_data_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.airports_data
    ADD CONSTRAINT airports_data_pkey PRIMARY KEY (airport_code);


--
-- Name: boarding_passes boarding_passes_flight_id_boarding_no_key; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.boarding_passes
    ADD CONSTRAINT boarding_passes_flight_id_boarding_no_key UNIQUE (flight_id, boarding_no);


--
-- Name: boarding_passes boarding_passes_flight_id_seat_no_key; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.boarding_passes
    ADD CONSTRAINT boarding_passes_flight_id_seat_no_key UNIQUE (flight_id, seat_no);


--
-- Name: boarding_passes boarding_passes_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.boarding_passes
    ADD CONSTRAINT boarding_passes_pkey PRIMARY KEY (ticket_no, flight_id);


--
-- Name: bookings bookings_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.bookings
    ADD CONSTRAINT bookings_pkey PRIMARY KEY (book_ref);


--
-- Name: flights flights_flight_no_scheduled_departure_key; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.flights
    ADD CONSTRAINT flights_flight_no_scheduled_departure_key UNIQUE (flight_no, scheduled_departure);


--
-- Name: flights flights_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.flights
    ADD CONSTRAINT flights_pkey PRIMARY KEY (flight_id);


--
-- Name: seats seats_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.seats
    ADD CONSTRAINT seats_pkey PRIMARY KEY (aircraft_code, seat_no);


--
-- Name: ticket_flights ticket_flights_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.ticket_flights
    ADD CONSTRAINT ticket_flights_pkey PRIMARY KEY (ticket_no, flight_id);


--
-- Name: tickets tickets_pkey; Type: CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.tickets
    ADD CONSTRAINT tickets_pkey PRIMARY KEY (ticket_no);


--
-- Name: boarding_passes boarding_passes_ticket_no_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.boarding_passes
    ADD CONSTRAINT boarding_passes_ticket_no_fkey FOREIGN KEY (ticket_no, flight_id) REFERENCES stg.ticket_flights(ticket_no, flight_id);


--
-- Name: flights flights_aircraft_code_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.flights
    ADD CONSTRAINT flights_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES stg.aircrafts_data(aircraft_code);


--
-- Name: flights flights_arrival_airport_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.flights
    ADD CONSTRAINT flights_arrival_airport_fkey FOREIGN KEY (arrival_airport) REFERENCES stg.airports_data(airport_code);


--
-- Name: flights flights_departure_airport_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.flights
    ADD CONSTRAINT flights_departure_airport_fkey FOREIGN KEY (departure_airport) REFERENCES stg.airports_data(airport_code);


--
-- Name: seats seats_aircraft_code_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.seats
    ADD CONSTRAINT seats_aircraft_code_fkey FOREIGN KEY (aircraft_code) REFERENCES stg.aircrafts_data(aircraft_code) ON DELETE CASCADE;


--
-- Name: ticket_flights ticket_flights_flight_id_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.ticket_flights
    ADD CONSTRAINT ticket_flights_flight_id_fkey FOREIGN KEY (flight_id) REFERENCES stg.flights(flight_id);


--
-- Name: ticket_flights ticket_flights_ticket_no_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.ticket_flights
    ADD CONSTRAINT ticket_flights_ticket_no_fkey FOREIGN KEY (ticket_no) REFERENCES stg.tickets(ticket_no);


--
-- Name: tickets tickets_book_ref_fkey; Type: FK CONSTRAINT; Schema: bookings; Owner: postgres
--

ALTER TABLE ONLY stg.tickets
    ADD CONSTRAINT tickets_book_ref_fkey FOREIGN KEY (book_ref) REFERENCES stg.bookings(book_ref);


--
-- PostgreSQL database dump complete
--


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
