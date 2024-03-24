with 
	stg_flights as (
    	select *
    	from stg.flights
	),

	dim_times as (
	    select *
	    from final.dim_time
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

	final_fct_flight_activities as (
	    select 
	        sf.flight_id as flight_nk,
	        sf.flight_no as flight_no,
	        dd1.date_id as scheduled_departure_date_local,
	        dd2.date_id as scheduled_departure_date_utc,
	        dt1.time_id as scheduled_departure_time_local,
	        dt2.time_id as scheduled_departure_time_utc,
	        dd3.date_id as scheduled_arrival_date_local,
	        dd4.date_id as scheduled_arrival_date_utc,
	        dt3.time_id as scheduled_arrival_time_local,
	        dt4.time_id as scheduled_arrival_time_utc,
	        da1.airport_id as departure_airport,
	        da2.airport_id as arrival_airport,
	        dac.aircraft_id as aircraft_code,
	        dd5.date_id as actual_departure_date_local,
	        dd6.date_id as actual_departure_date_utc,
	        dt5.time_id as actual_departure_time_local,
	        dt6.time_id as actual_departure_time_utc,
	        dd7.date_id as actual_arrival_date_local,
	        dd8.date_id as actual_arrival_date_utc,
	        dt7.time_id as actual_arrival_time_local,
	        dt8.time_id as actual_arrival_time_utc,
	        sf.status as status,
	        (sf.actual_departure - sf.scheduled_departure) as delay_departure,
	        (sf.actual_arrival - sf.scheduled_arrival) as delay_arrival,
	        (sf.actual_arrival - sf.actual_departure) as travel_time,
            LEAST(MIN(sf.created_at), 
                    MIN(da1.created_at),
                    MIN(da2.created_at),
                    MIN(dac.created_at)) AS created_at,
            GREATEST(MAX(sf.updated_at), 
                    MAX(da1.updated_at),
                    MAX(da2.updated_at),
                    MAX(dac.updated_at)) AS updated_at

	    from stg_flights sf
	    join dim_dates dd1
	        on dd1.date_actual = DATE(sf.scheduled_departure)
	    join dim_dates dd2
	        on dd2.date_actual = DATE(sf.scheduled_departure AT TIME ZONE 'UTC')
	    join dim_times dt1
	        on dt1.time_actual::time = (sf.scheduled_departure)::time
	    join dim_times dt2
	        on dt2.time_actual::time = (sf.scheduled_departure AT TIME ZONE 'UTC')::time
	    join dim_dates dd3
	        on dd3.date_actual = DATE(sf.scheduled_arrival)
	    join dim_dates dd4
	        on dd4.date_actual = DATE(sf.scheduled_arrival AT TIME ZONE 'UTC')
	    join dim_times dt3
	        on dt3.time_actual::time = (sf.scheduled_arrival)::time
	    join dim_times dt4
	        on dt4.time_actual::time = (sf.scheduled_arrival AT TIME ZONE 'UTC')::time
	    join dim_airports da1
	        on da1.airport_nk = sf.departure_airport
	    join dim_airports da2
	        on da2.airport_nk = sf.arrival_airport
	    join dim_aircrafts dac 
	        on dac.aircraft_nk = sf.aircraft_code    
	    join dim_dates dd5
	        on dd5.date_actual = DATE(sf.actual_departure)
	    join dim_dates dd6
	        on dd6.date_actual = DATE(sf.actual_departure AT TIME ZONE 'UTC')
	    join dim_times dt5
	        on dt5.time_actual::time = (sf.actual_departure)::time
	    join dim_times dt6
	        on dt6.time_actual::time = (sf.actual_departure AT TIME ZONE 'UTC')::time
	    join dim_dates dd7
	        on dd7.date_actual = DATE(sf.actual_arrival)
	    join dim_dates dd8
	        on dd8.date_actual = DATE(sf.actual_arrival AT TIME ZONE 'UTC')
	    join dim_times dt7
	        on dt7.time_actual::time = (sf.actual_arrival)::time
	    join dim_times dt8
	        on dt8.time_actual::time = (sf.actual_arrival AT TIME ZONE 'UTC')::time

        group by
	        sf.flight_id,
	        sf.flight_no,
	        dd1.date_id,
	        dd2.date_id,
	        dt1.time_id,
	        dt2.time_id,
	        dd3.date_id,
	        dd4.date_id,
	        dt3.time_id,
	        dt4.time_id,
	        da1.airport_id,
	        da2.airport_id,
	        dac.aircraft_id,
	        dd5.date_id,
	        dd6.date_id,
	        dt5.time_id,
	        dt6.time_id,
	        dd7.date_id,
	        dd8.date_id,
	        dt7.time_id,
	        dt8.time_id,
	        sf.status,
	        delay_departure,
	        delay_arrival,
	        travel_time
	)
	
	INSERT INTO "final".fct_flight_activity(
		flight_nk, 
		flight_no, 
		scheduled_departure_date_local, 
		scheduled_departure_date_utc, 
		scheduled_departure_time_local, 
		scheduled_departure_time_utc, 
		scheduled_arrival_date_local, 
		scheduled_arrival_date_utc, 
		scheduled_arrival_time_local, 
		scheduled_arrival_time_utc, 
		departure_airport, 
		arrival_airport, 
		aircraft_code, 
		actual_departure_date_local, 
		actual_departure_date_utc, 
		actual_departure_time_local, 
		actual_departure_time_utc, 
		actual_arrival_date_local, 
		actual_arrival_date_utc, 
		actual_arrival_time_local, 
		actual_arrival_time_utc, 
		status, 
		delay_departure, 
		delay_arrival, 
		travel_time, 
		created_at, 
		updated_at
	)
	
	select 
		* 
	from 
		final_fct_flight_activities;