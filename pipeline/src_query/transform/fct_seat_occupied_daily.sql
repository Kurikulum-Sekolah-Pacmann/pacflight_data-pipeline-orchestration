with 
	stg_flights as (
    	select *
    	from stg.flights
	),

	dim_dates as (
	    select *
	    from final.dim_date
	),
	
	dim_airports as (
	    select *
	    from final.dim_airport
	),
	
	dim_aircrafts as (
	    select * 
	    from final.dim_aircraft
	),
	
	stg_boarding_passes as (
	    select *
	    from stg.boarding_passes
	),
	
	stg_seats as (
	    select *
	    from stg.seats
	),
	
	cnt_seat_occupied as (
	    select
	        sf.flight_id,
	        count(seat_no) as seat_occupied
	    from stg_flights sf
	    join stg_boarding_passes sbp 
	        on sbp.flight_id = sf.flight_id
	    where
	        status = 'Arrived'
	    group by 1
	),
	
	cnt_total_seats as (
	    select
	        aircraft_code,
	        count(seat_no) as total_seat
	    from stg_seats
	    group by 1
	),
	
	final_fct_seat_occupied_daily as (
	    select 
	        dd.date_id as date_flight,
	        sf.flight_id as flight_nk,
	        sf.flight_no as flight_no,
	        da1.airport_id as departure_airport,
	        da2.airport_id as arrival_airport,
	        dac.aircraft_id as aircraft_code,
	        sf.status as status,
	        cts.total_seat as total_seat,
	        cso.seat_occupied as seat_occupied,
	        (cts.total_seat - cso.seat_occupied) as empty_seats
     
	    from stg_flights sf
	    join dim_dates dd 
	        on dd.date_actual = DATE(sf.actual_departure)
	    join dim_airports da1
	        on da1.airport_nk = sf.departure_airport
	    join dim_airports da2
	        on da2.airport_nk = sf.arrival_airport
	    join dim_aircrafts dac
	        on dac.aircraft_nk = sf.aircraft_code
	    join cnt_seat_occupied cso
	        on cso.flight_id = sf.flight_id
	    join cnt_total_seats cts 
	        on cts.aircraft_code = sf.aircraft_code
	)
	
INSERT INTO final.fct_seat_occupied_daily(
	date_flight, 
	flight_nk, 
	flight_no, 
	departure_airport, 
	arrival_airport, 
	aircraft_code, 
	status, 
	total_seat, 
	seat_occupied, 
	empty_seats
)

select 
	* 
from 
	final_fct_seat_occupied_daily

ON CONFLICT(date_flight, flight_nk) 
DO UPDATE SET
    flight_no = EXCLUDED.flight_no,
	departure_airport = EXCLUDED.departure_airport,
	arrival_airport = EXCLUDED.arrival_airport,
	aircraft_code = EXCLUDED.aircraft_code,
	status = EXCLUDED.status,
	total_seat = EXCLUDED.total_seat,
	seat_occupied = EXCLUDED.seat_occupied,
	empty_seats = EXCLUDED.empty_seats,
	updated_at = CURRENT_TIMESTAMP;