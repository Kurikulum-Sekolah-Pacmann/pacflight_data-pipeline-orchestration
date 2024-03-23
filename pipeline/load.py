import luigi
import logging
import pandas as pd
import time
import sqlalchemy
from datetime import datetime
from extract import Extract
from utils.db_conn import db_connection
from utils.read_sql import read_sql_file
from utils.concat_dataframe import concat_dataframes
from utils.copy_log import copy_log
from utils.delete_temp_data import delete_temp
from sqlalchemy.orm import sessionmaker
import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_LOAD_QUERY = os.getenv("DIR_LOAD_QUERY")
DIR_LOG = os.getenv("DIR_LOG")

class Load(luigi.Task):
    
    def requires(self):
        return Extract()
    
    def run(self):
         
        # Configure logging
        logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                            level = logging.INFO, 
                            format = '%(asctime)s - %(levelname)s - %(message)s')
        
        # Read Data to be load
        try:
            # Read csv
            aircrafts_data = pd.read_csv(self.input()[0].path)
            airports_data = pd.read_csv(self.input()[1].path)
            bookings = pd.read_csv(self.input()[2].path)
            tickets = pd.read_csv(self.input()[3].path)
            seats = pd.read_csv(self.input()[4].path)
            flights = pd.read_csv(self.input()[5].path)
            ticket_flights = pd.read_csv(self.input()[6].path)
            boarding_passes = pd.read_csv(self.input()[7].path)
            
            # Modify some columns.
            # Modify some columns because if they are not replaced it will result in an error
            aircrafts_data['model'] = aircrafts_data['model'].str.replace("'", '"')
            airports_data['airport_name'] = airports_data['airport_name'].str.replace("'", '"')
            airports_data['city'] = airports_data['city'].str.replace("'", '"')
            flights = flights.where(pd.notnull(flights), None)
            tickets['contact_data'] = tickets['contact_data'].str.replace("'", '"')
            
            logging.info(f"Read Extracted Data - SUCCESS")
            
        except Exception:
            logging.error(f"Read Extracted Data  - FAILED")
            raise Exception("Failed to Read Extracted Data")
        
        # Establish connections to DWH
        try:
            _, dwh_engine = db_connection()
            logging.info(f"Connect to DWH - SUCCESS")
            
        except Exception:
            logging.info(f"Connect to DWH - FAILED")
            raise Exception("Failed to connect to Data Warehouse")
        
        
        # Truncate all tables before load
        # This puropose to avoid errors because duplicate key value violates unique constraint
        try:
            # Read query
            truncate_query = read_sql_file(
                file_path = f'{DIR_LOAD_QUERY}/stg-truncate_tables.sql'
            )
            
            # Split the SQL queries if multiple queries are present
            truncate_query = truncate_query.split(';')

            # Remove newline characters and leading/trailing whitespaces
            truncate_query = [query.strip() for query in truncate_query if query.strip()]
            
            # Create session
            Session = sessionmaker(bind = dwh_engine)
            session = Session()

            # Execute each query
            for query in truncate_query:
                query = sqlalchemy.text(query)
                session.execute(query)
                
            session.commit()
            
            # Close session
            session.close()

            logging.info(f"Truncate staging tables - SUCCESS")
        
        except Exception:
            logging.error(f"Truncate staging tables - FAILED")
            
            raise Exception("Failed to Truncate Tables")
        
        
        # Record start time for loading tables
        start_time = time.time()  
        
        # Load Tables
        try:
            # Load aircraft tables    
            aircrafts_data.to_sql('aircrafts_data', 
                                  con = dwh_engine, 
                                  if_exists = 'append', 
                                  index = False, 
                                  schema = 'stg')
            logging.info(f"LOAD 'stg.aircrafts_data' - SUCCESS")
            
            
            # Load airports_data tables
            airports_data.to_sql('airports_data', 
                                 con = dwh_engine, 
                                 if_exists = 'append', 
                                 index = False, 
                                 schema = 'stg')
            logging.info(f"LOAD 'stg.airports_data' - SUCCESS")
            
            
            # Load bookings tables
            bookings.to_sql('bookings', 
                            con = dwh_engine, 
                            if_exists = 'append', 
                            index = False, 
                            schema = 'stg')
            logging.info(f"LOAD 'stg.bookings' - SUCCESS")
            
            
            # Load tickets tables
            tickets.to_sql('tickets', 
                           con = dwh_engine, 
                           if_exists = 'append', 
                           index = False, 
                           schema = 'stg')
            logging.info(f"LOAD 'stg.tickets' - SUCCESS")
            
            
            # Load seats tables
            seats.to_sql('seats', 
                         con = dwh_engine, 
                         if_exists = 'append', 
                         index = False, 
                         schema = 'stg')
            logging.info(f"LOAD 'stg.seats' - SUCCESS")
            
            
            # Load flights tables
            flights.to_sql('flights', 
                           con = dwh_engine, 
                           if_exists = 'append', 
                           index = False, 
                           schema = 'stg')
            logging.info(f"LOAD 'stg.flights' - SUCCESS")
            
            
            # Load tickets_flights tables
            ticket_flights.to_sql('ticket_flights', 
                                  con = dwh_engine, 
                                  if_exists = 'append', 
                                  index = False, 
                                  schema = 'stg')
            logging.info(f"LOAD 'stg.ticket_flights' - SUCCESS")
            
            
            # Load boarding_passes tables
            boarding_passes.to_sql('boarding_passes', 
                                   con = dwh_engine, 
                                   if_exists = 'append', 
                                   index = False, 
                                   schema = 'stg')
            logging.info(f"LOAD 'stg.boarding_passes' - SUCCESS")
            logging.info(f"LOAD All Tables To DWH Staging - SUCCESS")
    
           # Record end time for loading tables
            end_time = time.time()  
            execution_time = end_time - start_time  # Calculate execution time
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Success'],
                'execution_time': [execution_time]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
        
        except Exception:
            logging.error(f"Load tables to DWH Staging - FAILED")
            
            # Get summary
            summary_data = {
                'timestamp': [datetime.now()],
                'task': ['Load'],
                'status' : ['Failed'],
                'execution_time': [0]
            }

            # Get summary dataframes
            summary = pd.DataFrame(summary_data)
            
            # Write Summary to CSV
            summary.to_csv(f"{DIR_TEMP_DATA}/load-summary.csv", index = False)
            
            raise Exception('Failed Load Tables To ')

    def output(self):
        return [luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'),
                luigi.LocalTarget(f'{DIR_TEMP_DATA}/load-summary.csv')]
  
# Execute the functions when the script is run
if __name__ == "__main__":
    # Build the task
    luigi.build([Extract(),
                 Load()])
    
    # Concat temp extract summary to final summary
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/extract-summary.csv')
    )
    
    # Concat temp extract summary to final summary
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/load-summary.csv')
    )
    
    # Append log from temp to final log
    copy_log(
        source_file = f'{DIR_TEMP_LOG}/logs.log',
        destination_file = f'{DIR_LOG}/logs.log'
    )
    
    # Delete temp data
    delete_temp(
        directory = f'{DIR_TEMP_DATA}'
    )
    
    # Delete temp log
    delete_temp(
        directory = f'{DIR_TEMP_LOG}'
    )