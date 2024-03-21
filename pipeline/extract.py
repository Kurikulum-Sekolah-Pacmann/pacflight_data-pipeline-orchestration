import luigi
from datetime import datetime
import logging
import time
import pandas as pd
from utils.db_conn import db_connection
from utils.read_sql import read_sql_file
from utils.concat_dataframe import concat_dataframes
from utils.copy_log import copy_log
from dotenv import load_dotenv
import os

# Define DIR
DIR_ROOT_PROJECT = os.getenv("DIR_ROOT_PROJECT")
DIR_TEMP_LOG = os.getenv("DIR_TEMP_LOG")
DIR_TEMP_DATA = os.getenv("DIR_TEMP_DATA")
DIR_EXTRACT_QUERY = os.getenv("DIR_EXTRACT_QUERY")
DIR_LOG = os.getenv("DIR_LOG")


class Extract(luigi.Task):
    
    tables_to_extract = ['bookings.aircrafts_data', 
                         'bookings.airports_data', 
                         'bookings.boarding_passes', 
                         'bookings.bookings', 
                         'bookings.flights', 
                         'bookings.seats',
                         'bookings.ticket_flights',
                         'bookings.tickets']
    
    def requires(self):
        pass


    def run(self):
        # Create summary for extract task
        timestamp_data = [datetime.now()]
        task_data = ['Extract']
        status_data = []
        execution_time_data = []
        
        try:
            # Configure logging
            logging.basicConfig(filename = f'{DIR_TEMP_LOG}/logs.log', 
                                level = logging.INFO, 
                                format = '%(asctime)s - %(levelname)s - %(message)s')
            
            # Connect to PostgreSQL database
            conn_src, _, _, _ = db_connection()
            
            # Define the query using the SQL content
            extract_query = read_sql_file(
                file_path = f'{DIR_EXTRACT_QUERY}/all-tables.sql'
            )
            
            start_time = time.time()  # Record start time
            
            for index, table_name in enumerate(self.tables_to_extract):
                try:
                    # Read data into DataFrame
                    df = pd.read_sql_query(extract_query.format(table_name = table_name), conn_src)

                    # Write DataFrame to CSV
                    df.to_csv(f"{DIR_TEMP_DATA}/{table_name}.csv", index=False)
                    
                    # Log success message
                    logging.info(f"EXTRACT '{table_name}' - SUCCESS.")
                    
                except Exception:
                    # Log success message
                    logging.info(f"EXTRACT '{table_name}' - FAILED.")  
                      
                    raise Exception(f"Failed to extract '{table_name}' tables")
            
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
            summary.to_csv(f"{DIR_TEMP_DATA}/extract-summary.csv", index = False)
                    
        except Exception:
            start_time = time.time()  # Record start time
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
            summary.to_csv(f"{DIR_TEMP_DATA}/extract-summary.csv", index = False)
            
            # Write exception
            raise Exception(f"FAILED to execute EXTRACT TASK !!!")

        finally:
            # Close database conn_src
            if conn_src:
                conn_src.close()
                
    def output(self):
        outputs = []
        for table_name in self.tables_to_extract:
            outputs.append(luigi.LocalTarget(f'{DIR_TEMP_DATA}/{table_name}.csv'))
            
        outputs.append(luigi.LocalTarget(f'{DIR_TEMP_DATA}/extract-summary.csv'))
            
        outputs.append(luigi.LocalTarget(f'{DIR_TEMP_LOG}/logs.log'))
        return outputs
    
# Execute the functions when the script is run
if __name__ == "__main__":
    luigi.build([Extract()])
    
    concat_dataframes(
        df1 = pd.read_csv(f'{DIR_ROOT_PROJECT}/pipeline_summary.csv'),
        df2 = pd.read_csv(f'{DIR_TEMP_DATA}/extract-summary.csv')
    )
    
    copy_log(
        source_file = f'{DIR_TEMP_LOG}/logs.log',
        destination_file = f'{DIR_LOG}/logs.log'
    )