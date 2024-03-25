import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from pipeline.load import Load
from pipeline.utils.db_conn import db_connection
from pipeline.utils.read_sql import read_sql_file
from sqlalchemy.orm import sessionmaker
import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_TRANSFORM_QUERY = os.getenv("DIR_TRANSFORM_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Transform(luigi.Task):
    
    def requires(self):
        return Load()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Read query to be executed
        try:
            # Read query to truncate bookings schema in dwh
            truncate_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/truncate-fact-tables.sql'
            )

            # Read transform query to final schema
            dim_aircraft_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/dim_aircraft.sql'
            )
            
            dim_airport_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/dim_airport.sql'
            )
            
            dim_passenger_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/dim_passenger.sql'
            )
            
            dim_seat_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/dim_seat.sql'
            )
            
            fct_boarding_pass_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_boarding_pass.sql'
            )
            
            fct_booking_ticket_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_booking_ticket.sql'
            )
            
            fct_flight_activity_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_flight_activity.sql'
            )
            
            fct_seat_occupied_daily_query = read_sql_file(
                file_path = f'{DIR_TRANSFORM_QUERY}/fct_seat_occupied_daily.sql'
            )
            
            
            logging.info("Read Transform Query - SUCCESS")
            
        except Exception:
            logging.error("Read Transform Query - FAILED")
            raise Exception("Failed to read Transform Query")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        
        #----------------------------------------------------------------------------------------------------------------------------------------
        # Record start time for transform tables
        start_time = time.time()
        logging.info("==================================STARTING TRANSFROM DATA=======================================")  
               
        # Transform to dimensions tables
        try:
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()
            
            # Transform to final.dim_aircraft
            query = sqlalchemy.text(dim_aircraft_query)
            session.execute(query)
            logging.info("Transform to 'final.dim_aircraft' - SUCCESS")
            
            # Transform to final.dim_airport
            query = sqlalchemy.text(dim_airport_query)
            session.execute(query)
            logging.info("Transform to 'final.dim_airport' - SUCCESS")
            
            # Transform to final.dim_passenger
            query = sqlalchemy.text(dim_passenger_query)
            session.execute(query)
            logging.info("Transform to 'final.dim_passenger' - SUCCESS")
            
            # Transform to final.dim_seat
            query = sqlalchemy.text(dim_seat_query)
            session.execute(query)
            logging.info("Transform to 'final.dim_seat' - SUCCESS")
            
            # Truncate fact tables
            # Split the SQL queries if multiple queries are present
            truncate_query = truncate_query.split(';')

            # Remove newline characters and leading/trailing whitespaces
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            
            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)
            logging.info("Truncate Fact Tables - SUCCESS")
            
            # Transform to final.fct_boarding_pass
            query = sqlalchemy.text(fct_boarding_pass_query)
            session.execute(query)
            logging.info("Transform to 'final.fct_boarding_pass' - SUCCESS")
            
            # Transform to final.fct_booking_ticket
            query = sqlalchemy.text(fct_booking_ticket_query)
            session.execute(query)
            logging.info("Transform to 'final.fct_booking_ticket' - SUCCESS")
            
            # Transform to final.fct_flight_activity
            query = sqlalchemy.text(fct_flight_activity_query)
            session.execute(query)
            logging.info("Transform to 'final.fct_flight_activity' - SUCCESS")
            
            # Transform to final.fct_seat_occupied_daily
            query = sqlalchemy.text(fct_seat_occupied_daily_query)
            session.execute(query)
            logging.info("Transform to 'final.fct_seat_occupied_daily' - SUCCESS")
            
            # Commit transaction
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Transform to All Dimensions and Fact Tables - SUCCESS")
            
            # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index = False)
            
        except Exception:
            logging.error(f"Transform to All Dimensions and Fact Tables - FAILED")
        
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Transform'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/transform-summary.csv", index = False)
            
            logging.error("Transform Tables - FAILED")
            raise Exception('Failed Transforming Tables')   
        
        logging.info("==================================ENDING TRANSFROM DATA=======================================") 

    #----------------------------------------------------------------------------------------------------------------------------------------
    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/transform-summary.csv')]