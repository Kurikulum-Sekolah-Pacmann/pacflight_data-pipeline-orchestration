import luigi
import logging
import pandas as pd
import time
from datetime import datetime
from extract import Extract
from utils.db_conn import db_connection
from utils.read_sql import read_sql_file
from utils.concat_dataframe import concat_dataframes
from utils.copy_log import copy_log
from utils.format_null_value import format_value
import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Load(luigi.Task):
    
    current_local_time = datetime.now()
    
    def requires(self):
        return Extract()
    
    def run(self):
        # Create summary for extract task
        timestamp_data = [datetime.now()]
        task_data = ['Load']
        status_data = []
        execution_time_data = []
    
        # Establish connections to source and DWH databases
        try:
            _, _, conn_dwh, cur_dwh = db_connection()
            
        except Exception:
            raise Exception("Failed to connect to Data Warehouse")
        
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        # Data to be loaded
        try:
            aircrafts_data = pd.read_csv(self.input()[0].path)
            airports_data = pd.read_csv(self.input()[1].path)
            boarding_passes = pd.read_csv(self.input()[2].path)
            bookings = pd.read_csv(self.input()[3].path)
            flights = pd.read_csv(self.input()[4].path)
            flights = flights.where(pd.notnull(flights), None)
            seats = pd.read_csv(self.input()[5].path)
            ticket_flights = pd.read_csv(self.input()[6].path)
            tickets = pd.read_csv(self.input()[7].path)

        except Exception:
            raise Exception("Failed to Read Extracted CSV")
        
        
        # Define the query of each tables
        try:
            upsert_aircrafts_data_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-aircrafts_data.sql"
            )
            
            upsert_airports_data_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-airports_data.sql"
            )
            
            upsert_boarding_passes_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-boarding_passes.sql"
            )
            
            upsert_bookings_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-bookings.sql"
            )
            
            upsert_flights_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-flights.sql"
            )
            
            upsert_seats_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-seats.sql"
            )
            
            upsert_ticket_flights_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-ticket_flights.sql"
            )
            
            upsert_tickets_query = read_sql_file(
                file_path = f"{DIR_LOAD_QUERY}/stg-tickets.sql"
            )
            
        except Exception:
            raise Exception("Failed to read SQL Query")
        
        start_time = time.time()  # Record start time
        
        # Load to Database
        try:
            # Load to 'aircrafts_data' Table
            # for index, row in aircrafts_data.iterrows():
            #     # Extract values from the DataFrame row
            #     aircraft_code = row['aircraft_code']
            #     model = row['model'].replace("'", "\"")
            #     range = row['range']
            #     # created_at = row['created_at']
            #     # updated_at = row['updated_at']
            
            #     # Execute the upsert query
            #     cur_dwh.execute(upsert_aircrafts_data_query.format(
            #         aircraft_code = aircraft_code,
            #         model = model,
            #         range = range,
            #         # created_at = created_at,
            #         # updated_at = updated_at,
            #         current_local_time = self.current_local_time
            #     ))

            # # Commit the transaction
            # conn_dwh.commit()
            # # Close the cursor and connection
            # conn_dwh.close()
            # cur_dwh.close()
            # _, _, conn_dwh, cur_dwh = db_connection()
            
            # # Log success message
            # logging.info(f"LOAD aircrafts_data - SUCCESS")
            
            # # Load to 'airports_data' Table
            # for index, row in airports_data.iterrows():
            #     # Extract values from the DataFrame row
            #     airport_code = row['airport_code']
            #     airport_name = row['airport_name'].replace("'", "\"")
            #     city = row['city'].replace("'", "\"")
            #     coordinates = row['coordinates']
            #     timezone = row['timezone']
            #     # created_at = row['created_at']
            #     # updated_at = row['updated_at']
            
            #     # Execute the upsert query
            #     cur_dwh.execute(upsert_airports_data_query.format(
            #         airport_code = airport_code,
            #         airport_name = airport_name,
            #         city = city,
            #         coordinates = coordinates,
            #         timezone = timezone,
            #         # created_at = created_at,
            #         # updated_at = updated_at,
            #         current_local_time = self.current_local_time
            #     ))

            # # Commit the transaction
            # conn_dwh.commit()
            # # Close the cursor and connection
            # conn_dwh.close()
            # cur_dwh.close()
            # _, _, conn_dwh, cur_dwh = db_connection()
            
            # # Log success message
            # logging.info(f"LOAD airports_data - SUCCESS")
            
            # Load to 'bookings' Table
            for index, row in bookings.iterrows():
                # Extract values from the DataFrame row
                book_ref = row['book_ref']
                book_date = row['book_date']
                total_amount = row['total_amount']
                # created_at = row['created_at']
                # updated_at = row['updated_at']
            
                # Execute the upsert query
                cur_dwh.execute(upsert_bookings_query.format(
                    book_ref = book_ref,
                    book_date = book_date,
                    total_amount = total_amount,
                    # created_at = created_at,
                    # updated_at = updated_at,
                    current_local_time = self.current_local_time
                ))

            # Commit the transaction
            conn_dwh.commit()
            # Close the cursor and connection
            conn_dwh.close()
            cur_dwh.close()
            _, _, conn_dwh, cur_dwh = db_connection()
            
            # Log success message
            logging.info(f"LOAD bookings - SUCCESS")
            
            # # Load to 'tickets' Table
            # for index, row in tickets.iterrows():
            #     # Extract values from the DataFrame row
            #     ticket_no = row['ticket_no']
            #     book_ref = row['book_ref']
            #     passenger_id = row['passenger_id']
            #     passenger_name = row['passenger_name']
            #     contact_data = row['contact_data'].replace("'", "\"")
            #     # created_at = row['created_at']
            #     # updated_at = row['updated_at']
            
            #     # Execute the upsert query
            #     cur_dwh.execute(upsert_tickets_query.format(
            #         ticket_no = ticket_no,
            #         book_ref = book_ref,
            #         passenger_id = passenger_id,
            #         passenger_name = passenger_name,
            #         contact_data = contact_data,
            #         # created_at = created_at,
            #         # updated_at = updated_at,
            #         current_local_time = self.current_local_time
            #     ))
                
            # # Commit the transaction
            # conn_dwh.commit()
            
            # # Log success message
            # logging.info(f"LOAD tickets - SUCCESS")
            # # Close the cursor and connection
            # conn_dwh.close()
            # cur_dwh.close()
            # _, _, conn_dwh, cur_dwh = db_connection()
            
            # # Load to 'seats' Table
            # for index, row in seats.iterrows():
            #     # Extract values from the DataFrame row
            #     aircraft_code = row['aircraft_code']
            #     seat_no = row['seat_no']
            #     fare_conditions = row['fare_conditions']
            #     # created_at = row['created_at']
            #     # updated_at = row['updated_at']
            
            #     # Execute the upsert query
            #     cur_dwh.execute(upsert_seats_query.format(
            #         aircraft_code = aircraft_code,
            #         seat_no = seat_no,
            #         fare_conditions = fare_conditions,
            #         # created_at = created_at,
            #         # updated_at = updated_at,
            #         current_local_time = self.current_local_time
            #     ))

            # # Commit the transaction
            # conn_dwh.commit()
            
            # # Log success message
            # logging.info(f"LOAD seats - SUCCESS")
            
            # # Close the cursor and connection
            # conn_dwh.close()
            # cur_dwh.close()
            
            # _, _, conn_dwh, cur_dwh = db_connection()
            
            # # Load to 'flights' Table
            # for index, row in flights.iterrows():
            #     # Extract values from the DataFrame row
            #     flight_id = row['flight_id']
            #     flight_no = row['flight_no']
            #     scheduled_departure = row['scheduled_departure']
            #     scheduled_arrival = row['scheduled_arrival']
            #     departure_airport = row['departure_airport']
            #     arrival_airport = row['arrival_airport']
            #     status = row['status']
            #     aircraft_code = row['aircraft_code']
            #     actual_departure = row['actual_departure']
            #     # Convert None to NULL for timestamp column
            #     # actual_departure = str(actual_departure) if actual_departure is not 'NULL' else 'NULL'
            #     actual_arrival = row['actual_arrival']
            #     # created_at = row['created_at']
            #     # updated_at = row['updated_at']
            
            #     # Execute the upsert query
            #     cur_dwh.execute(upsert_flights_query.format(
            #         flight_id=flight_id,
            #         flight_no=flight_no,
            #         scheduled_departure=scheduled_departure,
            #         scheduled_arrival=scheduled_arrival,
            #         departure_airport=departure_airport,
            #         arrival_airport=arrival_airport,
            #         status=status,
            #         aircraft_code=aircraft_code,
            #         actual_departure=format_value(actual_departure),
            #         actual_arrival=format_value(actual_arrival),
            #         # created_at=created_at,
            #         # updated_at=updated_at,
            #         current_local_time=self.current_local_time
            #     ))

            # # Commit the transaction
            # conn_dwh.commit()
            # # Close the cursor and connection
            # conn_dwh.close()
            # cur_dwh.close()
            # _, _, conn_dwh, cur_dwh = db_connection()
            
            # # Log success message
            # logging.info(f"LOAD flights - SUCCESS")
            
            # # Load to 'ticket_flights' Table
            # for index, row in ticket_flights.iterrows():
            #     # Extract values from the DataFrame row
            #     ticket_no = row['ticket_no']
            #     flight_id = row['flight_id']
            #     fare_conditions = row['fare_conditions']
            #     amount = row['amount']
            #     # created_at = row['created_at']
            #     # updated_at = row['updated_at']
            
            #     # Execute the upsert query
            #     cur_dwh.execute(upsert_ticket_flights_query.format(
            #         ticket_no = ticket_no,
            #         flight_id = flight_id,
            #         fare_conditions = fare_conditions,
            #         amount = amount,
            #         # created_at = created_at,
            #         # updated_at = updated_at,
            #         current_local_time = self.current_local_time
            #     ))

            # # Commit the transaction
            # conn_dwh.commit()
            # # Close the cursor and connection
            # conn_dwh.close()
            # cur_dwh.close()
            # _, _, conn_dwh, cur_dwh = db_connection()
            
            # # Log success message
            # logging.info(f"LOAD ticket_flights - SUCCESS")
            
            # # Load to 'boarding_passes' Table
            # for index, row in boarding_passes.iterrows():
            #     # Extract values from the DataFrame row
            #     ticket_no = row['ticket_no']
            #     flight_id = row['flight_id']
            #     boarding_no = row['boarding_no']
            #     seat_no = row['seat_no']
            #     # created_at = row['created_at']
            #     # updated_at = row['updated_at']
            
            #     # Execute the upsert query
            #     cur_dwh.execute(upsert_boarding_passes_query.format(
            #         ticket_no = ticket_no,
            #         flight_id = flight_id,
            #         boarding_no = boarding_no,
            #         seat_no = seat_no,
            #         # created_at = created_at,
            #         # updated_at = updated_at,
            #         current_local_time = self.current_local_time
            #     ))

            # # Commit the transaction
            # conn_dwh.commit()
            # # Close the cursor and connection
            # conn_dwh.close()
            # cur_dwh.close()
            
            # # Log success message
            # logging.info(f"LOAD boarding_passes - SUCCESS")
    
           
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            status_data.append('Success')
            execution_time_data.append(execution_time)
            
            # Get summary dict
            summary_data = {
                'timestamp': timestamp_data,
                'task': task_data,
                'status' : status_data,
                'execution_time': execution_time_data
            }
            
            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write DataFrame to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
        
        except Exception:
            start_time = time.time() # Record start time
            end_time = time.time()  # Record end time
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            status_data.append('Failed')
            execution_time_data.append(execution_time)
            
            # Get summary dict
            summary_data = {
                'timestamp': timestamp_data,
                'task': task_data,
                'status' : status_data,
                'execution_time': execution_time_data
            }
            
            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write DataFrame to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
            raise Exception('Failed to Load Tables')

    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv')]
  
# Execute the functions when the script is run
if __name__ == "__main__":
    luigi.build([Extract(),
                 Load()])
    
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/extract-summary.csv')
    )
    
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/load-summary.csv')
    )
    
    copy_log(
        source_file = f'{DIR_TEMP_LOG}/logs.log',
        destination_file = f'{DIR_LOG}/logs.log'
    )