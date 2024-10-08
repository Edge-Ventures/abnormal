import os, sys
import json
import sqlite3
from uuid import UUID
from datetime import datetime
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, MetaData, Table, func, text
from sqlalchemy.exc import SQLAlchemyError

from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Access the environment variables
DB_USER = os.getenv('DB_USER')
DB_PASS =  os.getenv('DB_PASS')
DB_SERVER = os.getenv('DB_SERVER')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')
DATABASE_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_SERVER}:{DB_PORT}/{DB_NAME}"
CONNECTION_STRING = DATABASE_URL
SQLITE_STORAGE_DATABASE_NAME = "./api/snapshots.db"
SCHEMA = "squabble"
TABLE_LIST = ["debate", "participant"]

class DataSnapshot():
    def __init__(self, connection_string, tables=[], schema=None):

        self.tables = tables
        self.schema = schema    
        self.storage_database_name = SQLITE_STORAGE_DATABASE_NAME  
        self.source_engine = create_engine(connection_string)
        self.storage_engine = create_engine(f'sqlite:///{self.storage_database_name}')


    def get_column_aggregations(self, df, dataset_name):
        aggregations = []
        print("DATASET NAME: ",dataset_name)
        for column in df.columns:
            data_type = df[column].dtype
            count = df[column].count()
            count_null = df[column].isnull().sum()
            null_percentage = (count_null / len(df)) * 100
            unique_values = df[column].nunique()

            # Initialize values for numeric columns
            min_value = np.nan
            percentile_25 = np.nan
            percentile_50 = np.nan
            median_value = np.nan
            percentile_75 = np.nan
            max_value = np.nan
            mean_value = np.nan
            std_value = np.nan
            skew_value = np.nan

            if np.issubdtype(data_type, np.number):
                min_value = df[column].min()
                percentile_25 = df[column].quantile(0.25)
                percentile_25 = df[column].quantile(0.5)
                median_value = df[column].median()
                percentile_75 = df[column].quantile(0.75)
                max_value = df[column].max()
                mean_value = df[column].mean()
                std_value = df[column].std()
                skew_value = df[column].skew()



            aggregations.append({
                'dataset_name': dataset_name,
                'column_name': column,
                'data_type': data_type,
                'count': count,
                'count_of_null_records': count_null,
                'null_percentage': null_percentage,
                'unique_values': unique_values,
                'min_value': min_value,
                'percentile_25': percentile_25,
                'percentile_50': percentile_50,
                'percentile_75': percentile_75,
                'max_value': max_value,
                'mean': mean_value,
                'median': median_value,
                'std': std_value,
                'skew': skew_value,
                'snapshot_created_at': datetime.now()
            })

        return pd.DataFrame(aggregations)





    def create_snapshots_table(self):

        db_path=self.storage_database_name

        db_connection_string = f'sqlite:///{db_path}'
        engine = create_engine(db_connection_string)
        
        ddl = """
        CREATE TABLE IF NOT EXISTS snapshots (
            dataset_name TEXT NOT NULL,
            column_name TEXT NOT NULL,
            data_type TEXT NOT NULL,
            count INTEGER,
            count_of_null_records INTEGER,
            null_percentage REAL,
            unique_values INTEGER,
            min_value REAL,
            percentile_25 REAL,
            percentile_50 REAL,
            percentile_75 REAL,
            max_value REAL,
            mean REAL,
            median REAL,
            std REAL,
            skew REAL,
            snapshot_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with engine.connect() as connection:
            connection.execute(text(ddl))




    def store_column_aggregations_results(self, df):

        # db_path=self.storage_database_name

        try:
            # Check if the database exists
            # db_exists = os.path.exists(db_path)
            
            # # Create SQLAlchemy engine
            # db_connection_string = f'sqlite:///{db_path}'
            # engine = create_engine(db_connection_string)
            
            # Convert dtypes to strings and ensure numeric values are float or int
            df = df.astype({
                'dataset_name': str,
                'column_name': str,
                'data_type': str,
                'count': int,
                'count_of_null_records': int,
                'null_percentage': float,
                'unique_values': int,
                'min_value': float,
                'percentile_25': float,
                'percentile_50': float,
                'percentile_75': float,
                'max_value': float,
                'mean': float,
                'median': float,
                'std': float,
                'skew': float,
                'snapshot_created_at': str
            })

            # Replace NaN with None
            df = df.replace({np.nan: None})

            # Convert any datetime objects to strings
            for col in df.select_dtypes(include=['datetime64']).columns:
                df[col] = df[col].astype(str)

            # Verify the number of columns in the DataFrame
            #print(f"Number of columns in DataFrame: {len(df.columns)}")


            # print(df.dtypes)
            # print(df.head())

            # Store the DataFrame into a SQL table called 'snapshots'
            df.to_sql('snapshots', con=self.storage_engine, if_exists='append', index=False)
            
            # if not db_exists:
            #     print(f"Database created at {db_path}")
            # else:
            #     print(f"Data appended to the existing database at {db_path}")

        except SQLAlchemyError as e:
            print(f"An error occurred while interacting with the database: {str(e)}")
        except Exception as e:
            print(f"An unexpected error occurred: {str(e)}")
        finally:
            return df




    def create_snapshots_threshold_table(self):

        db_path=self.storage_database_name

        db_connection_string = f'sqlite:///{db_path}'
        engine = create_engine(db_connection_string)
        
        ddl = """
        CREATE TABLE IF NOT EXISTS snapshots_threshold (
            dataset_name TEXT NOT NULL,
            snapshot_field TEXT NOT NULL,
            source_field_name TEXT NOT NULL,
            data_type TEXT NOT NULL,
            lower_bound REAL,
            upper_bound REAL,
            snapshot_created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        with engine.connect() as connection:
            connection.execute(text(ddl))




    # Plan
    # Connect to the SQLite database.
    # Retrieve data from the snapshots table.
    # Group the data by dataset_name.
    # For each group, analyze each numeric column to determine the upper and lower outlier ranges.
    # Store the results in the snapshots_threshold table.



    def calculate_outlier_bounds(self, series):
        q1 = series.quantile(0.25)
        q3 = series.quantile(0.75)
        iqr = q3 - q1
        lower_bound = q1 - 1.5 * iqr
        upper_bound = q3 + 1.5 * iqr
        return lower_bound, upper_bound




    def analyze_and_store_thresholds(self, df):

        
        # Group by dataset_name
        grouped = df.groupby(['dataset_name', 'column_name'])
        #grouped = df.groupby('dataset_name')
        
        results = []

        for (dataset_name, source_field), group in grouped:
            # print(f"Processing dataset: {dataset_name}")
            # print(f"Source Field: {source_field}")
            # # print(f"Group: {group.head()}")
            # print("=====================================")
            # print("group.columns", group.columns)   
        
            for column in group.columns:
                if group[column].dtype in [np.float64, np.int64, np.float32, np.int32]:                
                    lower_bound, upper_bound = self.calculate_outlier_bounds(group[column])
                    # Delete previous data for the same dataset_name
                    with self.storage_engine.connect() as connection:
                        sql = """
                                DELETE FROM snapshots_threshold 
                                WHERE dataset_name = :dataset_name 
                                and source_field_name = :source_field
                                and snapshot_field = :snapshot_field
                                """
                        fields = {'dataset_name': dataset_name, 'source_field': source_field, 'snapshot_field': column}
                        delete_query = text(sql)

                        connection.execute(delete_query, fields)
                        connection.commit()

                    #for original_field in group[column]:
                    results.append({
                        'dataset_name': dataset_name,
                        'snapshot_field': column,
                        'source_field_name': str(source_field),
                        'data_type': str(group[column].dtype),
                        'lower_bound': lower_bound,
                        'upper_bound': upper_bound,
                        'snapshot_created_at': pd.Timestamp.now()
                    })
        
        # Convert results to DataFrame
        results_df = pd.DataFrame(results)
        
        # # Store results in snapshots_threshold table
        results_df.to_sql('snapshots_threshold', con=self.storage_engine, if_exists='append', index=False)
        return results_df



    # monitor the table. print alerts. 
    # if the value is outside the bounds, print an alert.

    # Connect to the SQLite database
    def dataset_check(self):

        conn = sqlite3.connect(self.storage_database_name)

        # Query the snapshot and snapshot_threshold tables
        query ="""

            with latest_recs as (
            select max(snapshot_created_at) latest_snapshot_date
            , dataset_name
            , column_name  
            from snapshots
            group by dataset_name, column_name  
            )

            select 
                s.dataset_name 
                , s.column_name
                , s.data_type
                , s.count
                , s.count_of_null_records
                , s.null_percentage
                , s.unique_values
                , s.min_value
                , s.percentile_25
                , s.percentile_50
                , s.percentile_75
                , s.max_value
                , s.mean
                , s.median
                , s.std
                , s.skew
                , s.snapshot_created_at
            from snapshots s
            join latest_recs lr on lr.latest_snapshot_date = snapshot_created_at 
                                    and lr.dataset_name = s.dataset_name 
                                    and lr.column_name = s.column_name 
            where  s.dataset_name  = 'csv.earthquake.csv.earthquakes'
            and s.column_name  in ('Latitude','DateTime')



        """.format()
        # print(query)
        result = conn.execute(query).fetchall()

        # check each column associated to a dataset. 
        # snapshot table has a row per source column per snapshot timestamp. 
        # check each coloumn value per datasetname. get the each column value, 
        # check to see if the latest value is outside the bounds.
        # Check if any values have breached the threshold


        breached_values = []
        dataset =  []
        for row in result:
            dataset_name = row[0]
            column_name = row[1]
            data_type = row[2]
            count = row[3]
            count_of_null_records = row[4]
            null_percentage = row[5]
            unique_values = row[6]
            min_value = row[7]
            percentile_25 = row[8]
            percentile_50 = row[9]
            percentile_75 = row[10]
            max_value = row[11]
            mean = row[12]
            median = row[13]
            std = row[14]
            skew = row[15]
            snapshot_created_at = row[16]

            row_dict = {
                'count': count,
                'count_of_null_records': count_of_null_records,
                'null_percentage': null_percentage,
                'unique_values': unique_values,
                'min_value': min_value,
                'percentile_25': percentile_25,
                'percentile_50': percentile_50,
                'percentile_75': percentile_75,
                'max_value': max_value,
                'mean': mean,
                'median': median,
                'std': std,
                'skew': skew,
            }
            dataset.append(row_dict) 
            # print("ROW DICT: ",row_dict)

            for key in row_dict.keys():
                # print("KEY: ",key)
                for column in row_dict:
                    if key == column and min_value is not None and max_value is not None:
                        # print("COLUMN:",column)
                        threshold_query = f"""
                                        -- key :{key}, column: {column}, value: {row_dict[column]} 

                                            SELECT 
                                            lower_bound
                                            , upper_bound
                                            FROM snapshots_threshold 
                                            WHERE dataset_name = '{dataset_name}' 
                                            AND snapshot_field = '{key}'
                                            AND source_field_name = '{column_name}'"""
                        threshold_result = conn.execute(threshold_query).fetchone()

                        # Check if the latest value is outside the bounds
                        if threshold_result is not None:
                            lower_bound, upper_bound = threshold_result
                            current_value = row_dict[column]
                            if current_value < lower_bound or current_value > upper_bound:
                                print("BREACHED")
                                rint("THRESHOLD RESULT: ",threshold_result)
                                print(f"key :{key}, column: {column}, value: {row_dict[column]}")
                                breached_values.append((dataset_name, column_name, snapshot_created_at, min_value, max_value, lower_bound, upper_bound))


        # Print the breached values
        if breached_values:
            print("Breached values:")
            for value in breached_values:
                print(value)
        else:
            print("No breached values found.")



    def monitor_tables(self):

        for current_table in self.tables:

            # # check the latest dataset and determine if any thresholds have been breached 
            dataset_name = str(self.source_engine.url).split("/")[-1] + "." + self.schema +  "." + current_table

            df = pd.read_sql_table(table_name=current_table, con=self.source_engine,schema=SCHEMA)

            # #get the column aggregations.
            aggregate_df = self.get_column_aggregations(df, current_table)    

            # #save the aggregate dataset. - save a snapshot
            stored = self.store_column_aggregations_results(aggregate_df)

            # check for anomalies of the recent snapshot
            print("first dataset check:")
            self.dataset_check()

            # create new thresholds - Analyze and store thresholds of the dataset
            # outlier data is in the snapshots_threshold table
            analyzed = self.analyze_and_store_thresholds(stored)

            # check for anomalies of the recent snapshot
            print("second dataset check:")
            self.dataset_check()





if __name__ == "__main__":

    print("CONNECTION_STRING: ",CONNECTION_STRING)
    print("TABLE_LIST: ",TABLE_LIST)
    print("SCHEMA: ",SCHEMA)

    abnormal = DataSnapshot(connection_string=CONNECTION_STRING, tables=TABLE_LIST, schema=SCHEMA)
    print(abnormal)
    print(abnormal.source_engine)
    print(abnormal.storage_engine)
    print(abnormal.storage_database_name)
    abnormal.monitor_tables()