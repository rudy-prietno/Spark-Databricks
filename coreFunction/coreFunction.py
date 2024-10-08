# Databricks notebook source
import os
import pandas as pd

import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv
from singleton_decorator import singleton

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql import functions as F

from ydata_profiling import ProfileReport, compare

import time
import decimal
import hashlib
from datetime import datetime, timezone, timedelta

import warnings
warnings.filterwarnings("ignore", category=RuntimeWarning)


try:
    spark
except NameError:
    spark = SparkSession.builder \
        .appName("ETL or ELT App") \
        .getOrCreate()


def convert_to_decimal(value):
    if pd.isna(value):
        return None
    return decimal.Decimal(str(round(value, 2)))


class PostgreSQLConnectionError(Exception):
    """Custom exception for PostgreSQL connection errors."""
    pass


@singleton
class PostgreSQLConnector:
    """
    Singleton class to handle PostgreSQL connections.

    This ensures that only one instance of the connection is created throughout
    the application lifecycle to save resources.
    """

    def __init__(self):
        """Initializes the PostgreSQLConnector by establishing a connection."""
        self.conn = None
        self._validate_credentials()
        self._create_connection()

    def _validate_credentials(self):
        """
        Validates the necessary environment variables for PostgreSQL credentials.
        Raises:
            PostgreSQLConnectionError: If any required credentials are missing.
        """
        required_vars = ['POSTGRES_HOST', 'POSTGRES_DB', 'POSTGRES_USER', 'POSTGRES_PASSWORD', 'POSTGRES_PORT']
        for var in required_vars:
            if not os.getenv(var):
                raise PostgreSQLConnectionError(f"Environment variable {var} is missing.")

    def _create_connection(self):
        """
        Establishes the PostgreSQL connection using psycopg2.
        """
        try:
            self.conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                port=os.getenv('POSTGRES_PORT')
            )
        except psycopg2.Error as e:
            raise PostgreSQLConnectionError(f"Error connecting to PostgreSQL: {str(e)}")

    def get_connection(self):
        """Returns the active PostgreSQL connection."""
        return self.conn
        

class DataReader:
    """
    Class to read data from Excel and PostgreSQL.

    Attributes:
        file_path (str): Path to the Excel file.
        sheet_name (str): Name of the sheet to read.
        query (str): SQL query to execute for PostgreSQL.
    """

    def __init__(self, file_path=None, sheet_name=None, query=None):
        """Initializes DataReader with optional file path, sheet name, and SQL query."""
        self.file_path = file_path
        self.sheet_name = sheet_name
        self.query = query
        self.data = None

    def read_excel(self):
        """
        Reads the Excel file into a Pandas DataFrame.
        
        Returns:
            pd.DataFrame: DataFrame containing the data from the Excel file.
        """
        if self.file_path and self.sheet_name:
            self.data = pd.read_excel(self.file_path, sheet_name=self.sheet_name)
        else:
            raise ValueError("Both file_path and sheet_name must be provided.")
        return self.data

    def read_postgresql(self):
        """
        Executes a SQL query on PostgreSQL and fetches the results.

        Returns:
            list: List of tuples representing the query result rows.
        """
        if not self.query:
            raise ValueError("SQL query must be provided.")
        
        conn = PostgreSQLConnector().get_connection()
        cursor = conn.cursor()

        try:
            cursor.execute(self.query)
            self.data = cursor.fetchall()
        except psycopg2.Error as e:
            raise PostgreSQLConnectionError(f"Failed to execute query: {str(e)}")
        finally:
            cursor.close()

        return self.data
        

class DataProfiling:
    """
    A class used for creating data profiles and comparing them using ydata_profiling.

    Attributes:
        titleProfile (str): The title for the profiling report.

    Methods:
        profile(dataFrame: pd.DataFrame, title: str) -> ProfileReport:
            Generates a profile report for the provided DataFrame.
        
        compare_profiles(profile1: ProfileReport, profile2: ProfileReport, output_file: str) -> None:
            Compares two profile reports and outputs the result as an HTML file.
    """

    # Disable analytics by setting environment variable
    os.environ['YDATA_PROFILING_NO_ANALYTICS'] = 'True'

    def __init__(self, titleProfile: str):
        """
        Initializes the DataProfiling class with a title for the profile report.

        Args:
            titleProfile (str): The title for the profiling report.
        """
        self.titleProfile = titleProfile


    def profile(self, dataFrame: pd.DataFrame) -> ProfileReport:
        """
        Generates a profile report for the provided DataFrame.

        Args:
            dataFrame (pd.DataFrame): The DataFrame for which the profile report is generated.


        Returns:
            ProfileReport: The generated profiling report.
        """
        profile = ProfileReport(dataFrame, title=self.titleProfile, explorative=True, minimal=True)
        return profile
    

    @staticmethod
    def compareProfiles(profile1: ProfileReport, profile2: ProfileReport, output_file: str) -> None:
        """
        Compares two profile reports and outputs the result as an HTML file.

        Args:
            profile1 (ProfileReport): The first profile report to compare.
            profile2 (ProfileReport): The second profile report to compare.
            output_file (str): The path to save the comparison report as an HTML file.

        Returns:
            None
        """
        # Compare the two profiles
        comparison_report = compare([profile1, profile2])
        
        # Save the comparison report to an HTML file
        comparison_report.to_file(output_file)

        print(f"Comparison report saved to {output_file}")


    @staticmethod
    def compareProfiles(profile1, profile2) -> None:
        """
        Compares two profile reports and outputs the result as an HTML file.

        Args:
            profile1 (ProfileReport): The first profile report to compare.
            profile2 (ProfileReport): The second profile report to compare.
            output_file (str): The comparison report as an HTML.

        Returns:
            display(comparison_report)
        """
        # Compare the two profiles
        comparison_report = compare([profile1, profile2])
        
        # Display the comparison report to an HTML
        return display (comparison_report)
    

class DataIngestion:
    
    def DeltaTables(tableName, dataFrameSource, primaryKey=None):
        """
        Ingests data into a Delta table using the provided DataFrame and primary key for merging.
        If no primary key is provided or the primary key is not unique, the data will be appended only.

        Parameters:
        - tableName (str): The name of the Delta table.
        - dataFrameSource (DataFrame): The source DataFrame containing data to be ingested.
        - primaryKey (str, optional): The primary key column used for merge operations.

        """

        # Set Spark configurations for Delta Lake optimizations
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.databricks.delta.merge.optimizeInsertDelete.enabled", "true")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")

        # Allow schema evolution during merge operations
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        try:
            # Check if Delta table exists
            deltaTable = DeltaTable.forName(spark, tableName)
            print(f"{tableName} is a Delta table.")
            
            if primaryKey:
                # Check if primaryKey is unique in the source DataFrame
                distinct_count = dataFrameSource.select(primaryKey).distinct().count()
                total_count = dataFrameSource.count()

                if distinct_count == total_count:
                    print(f"Primary key {primaryKey} is unique. Starting merge into Delta table.")
                    
                    # Perform merge operation
                    deltaTable.alias("tgt").merge(
                        dataFrameSource.alias("src"),
                        f"tgt.{primaryKey} = src.{primaryKey}"
                    ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

                    # Track update and insert operations
                    last_operation = deltaTable.history(1).select("operationMetrics").collect()[0]
                    update_count = last_operation["operationMetrics"]["numTargetRowsUpdated"]
                    insert_count = last_operation["operationMetrics"]["numTargetRowsInserted"]

                    print(f"Number of rows updated: {update_count}")
                    print(f"Number of rows inserted: {insert_count}")

                else:
                    # If primary key is not unique, do append-only
                    print(f"Primary key {primaryKey} is not unique. Appending data to {tableName}.")
                    dataFrameSource.write.format("delta").mode("append").saveAsTable(tableName)
                    row_count = dataFrameSource.count()
                    print(f"{row_count} rows appended to {tableName}.")

            else:
                # If no primary key is provided, do append-only
                print("No primary key provided. Appending data to the Delta table.")
                dataFrameSource.write.format("delta").mode("append").saveAsTable(tableName)
                row_count = dataFrameSource.count()
                print(f"{row_count} rows appended to {tableName}.")

        except Exception as e:
            print(f"Error: {str(e)}")
            print(f"{tableName} is not a Delta table, creating a new Delta table.")

            # If table doesn't exist, write as new Delta table
            row_count = dataFrameSource.count()
            dataFrameSource.write.format("delta").mode("overwrite").saveAsTable(tableName)
            print(f"{row_count} rows written to {tableName}.")


    def DeltaTablesPartition (queries, tableName, partitionKeys, orderKeys, partitionColumns, partitionColumnSecondary):
        """
        Ingests data into a Delta table, partitioning by specified columns, and handles merging or creating a new table if not found.

        Parameters:
        - queries (str): SQL query to retrieve the source DataFrame.
        - tableName (str): Name of the Delta table.
        - partitionKeys (list): List of columns to partition by.
        - orderKeys (list): List of columns to order by.
        - partitionColumns (str): Primary partition column.
        - partitionColumnSecondary (str): Secondary partition column for fallback in case of NULL values.

        """

        # Set Spark configurations for Delta Lake optimizations
        spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.databricks.delta.merge.optimizeInsertDelete.enabled", "true")
        spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
        
        # Allow schema evolution during merge operations
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # Standard timestamp write on dataframe
        spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

        # Execute the provided SQL query to get the source DataFrame
        rsDf = spark.sql(queries)

        # Create a WindowSpec with multiple partition and order columns
        windowSpec = Window.partitionBy([F.col(c) for c in partitionKeys])\
                        .orderBy([F.col(c).desc() for c in orderKeys])

        df_source = rsDf.withColumn("row_number", F.row_number().over(windowSpec))\
                        .filter("row_number = 1").drop("row_number")

        try:
            # Check if the Delta table already exists
            deltaTable = DeltaTable.forName(spark, tableName)
            print(f"{tableName} is a Delta table.")

            # Ensure the partition column is created in the source DataFrame
            dfF1 = df_source.withColumn(
                f'{partitionColumns}_Partition', 
                F.coalesce(F.to_date(F.col(f'{partitionColumns}')), F.to_date(F.col(f'{partitionColumnSecondary}')))
                )
            
            # Perform the merge operation
            print("Starting merge into Delta table.")
            deltaTable.alias("tgt").merge(
                dfF1.alias("src"),
                " AND ".join([f"tgt.{key} = src.{key}" for key in partitionKeys]) + 
                f" AND tgt.{partitionColumns}_Partition = src.{partitionColumns}_Partition"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

            # Track update and insert operations
            last_operation = deltaTable.history(1).select("operationMetrics").collect()[0]
            update_count = last_operation["operationMetrics"]["numTargetRowsUpdated"]
            insert_count = last_operation["operationMetrics"]["numTargetRowsInserted"]

            print(f"Number of rows updated: {update_count}")
            print(f"Number of rows inserted: {insert_count}")

        except Exception as e:
            print(f"Delta table not found or error occurred: {str(e)}")
            print("Creating new Delta table and writing data...")

            # Ensure the partition column is created in the source DataFrame
            dfF = df_source.withColumn(
                f'{partitionColumns}_Partition', 
                F.coalesce(F.to_date(F.col(f'{partitionColumns}')), F.to_date(F.col(f'{partitionColumnSecondary}')))
            )

            # Create a new Delta table with the specified partition column
            dfF.write.format("delta").mode('overwrite')\
                .option("mergeSchema", "true")\
                .option("overwriteSchema", "true")\
                .partitionBy(f'{partitionColumns}_Partition')\
                .saveAsTable(f'{tableName}')
            
            row_count = dfF.count()
            print(f"{row_count} rows written to {tableName}")


    def checkPartitionDelta (pathTable):
        """
        Checks the partitions of a Delta table, providing details about the number of files and the total size of each partition.

        Parameters:
        - pathTable (str): The path to the Delta table to be inspected.

        """
        
        try:
            # Use the DeltaTable class to access the Delta table
            delta_table_path = f'{pathTable}'
            table_name = delta_table_path.rstrip('/').split('/')[-1]
            print("Table Name:", table_name)

            # Load the Delta table
            deltaTable = DeltaTable.forPath(spark, delta_table_path)

            # Describe partitions for Delta table
            partition_info = spark.sql(f"DESCRIBE DETAIL '{delta_table_path}'").select('partitionColumns').collect()[0][0]

            if not partition_info:
                print("No partitions found in this Delta table.")
                return

            print(f"Partition columns: {partition_info}")

            # Get partition details
            df = spark.read.format("delta").load(delta_table_path)
            partitions_df = df.groupBy(partition_info).count()

            # Collect partition details and show the partition counts
            partitions_df.show(truncate=False)

            # Get file size and count of files in partitions
            partition_details = {}

            files_in_partition = dbutils.fs.ls(delta_table_path)
            for file in files_in_partition:
                if file.isDir():
                    partition_name = file.name
                    partition_path = file.path

                    files = dbutils.fs.ls(partition_path)
                    total_size = sum(f.size for f in files if not f.isDir())
                    file_count = len([f for f in files if not f.isDir()])
                    
                    partition_details[partition_name] = {
                        "size_bytes": total_size,
                        "file_count": file_count
                    }

            for partition_name, details in partition_details.items():
                print(f"Partition: {partition_name}, Size: {details['size_bytes']} bytes, Number of files: {details['file_count']}")
        
        except Exception as e:
            print(f"Delta table partition not found or error occurred: {str(e)}")


    def get_total_storage_size(path):
        """
        Calculates the total storage size of all files in the given path.

        Parameters:
        - path (str): The file system path to calculate the total size.

        Returns:
        - total_size (int): The total size of all files in the directory, in bytes.
        """

        total_size = 0
        files = dbutils.fs.ls(path)
        for file in files:
            total_size += file.size
        return total_size
    

    def maintenanceDelta(pathTable):
        """
        Performs maintenance on a Delta table by running the VACUUM command and measuring storage size.

        Parameters:
        - pathTable (str): Path to the Delta table to be maintained.
        
        """

        delta_table_path = pathTable

        # Measure total storage before VACUUM
        storage_before_vacuum = get_total_storage_size(delta_table_path)
        print(f"Total storage before VACUUM: {storage_before_vacuum / (1024 * 1024):.2f} MB")

        # Run the VACUUM command
        deltaTable = DeltaTable.forPath(spark, delta_table_path)  # Use DeltaTable to reference the Delta table
        deltaTable.vacuum(retentionHours=168)  # 168 hours = 7 days, VACUUM to remove old data files

        # Measure total storage after VACUUM
        storage_after_vacuum = get_total_storage_size(delta_table_path)
        print(f"Total storage after VACUUM: {storage_after_vacuum / (1024 * 1024):.2f} MB")

        # Calculate the difference in storage
        storage_reduction = (storage_before_vacuum - storage_after_vacuum) / (1024 * 1024)
        print(f"Storage reduced by: {storage_reduction:.2f} MB")


    def optimizeDelta(pathTable, queries, optimize_sql):
        """
        Optimizes a Delta table using VOrder combined with ZOrder and measures query performance before and after.
        
        Parameters:
        - pathTable (str): Path to the Delta table.
        - queries (str): SQL query to execute before and after optimization.
        - optimize_sql (str): SQL query to perform the Delta table optimization (e.g., OPTIMIZE ... ZORDER BY ...).
        
        """

        # Path and table details
        delta_table_path = pathTable
        table_name = delta_table_path.rstrip('/').split('/')[-1]

        print(f'Optimization Process for Table: {table_name}')

        # Describe details before optimization
        print("Before Optimization:")
        before_optimization = spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`")
        before_optimization.show(truncate=False)

        # Run a query before optimization and measure the time
        utc_now = datetime.now(timezone.utc)
        local_time_before = utc_now.astimezone(timezone(timedelta(hours=7)))  # Convert UTC to WIB (UTC+7)
        print(f"Query started at (WIB): {local_time_before.strftime('%Y-%m-%d %H:%M:%S')}")

        start_time = time.time()
        query_result_before = spark.sql(queries)
        query_result_before.show()  # Show the query result (optional)
        query_time_before = time.time() - start_time

        utc_now_after = datetime.now(timezone.utc)
        local_time_after = utc_now_after.astimezone(timezone(timedelta(hours=7)))  # Convert UTC to WIB (UTC+7)
        print(f"Query finished at (WIB): {local_time_after.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Query execution time before optimization: {query_time_before} seconds")

        # Perform optimization
        print("\nRunning OPTIMIZE command:")
        spark.sql(optimize_sql)

        # Describe details after optimization
        print("\nAfter Optimization:")
        after_optimization = spark.sql(f"DESCRIBE DETAIL delta.`{delta_table_path}`")
        after_optimization.show(truncate=False)

        # Run the same query after optimization and measure the time
        utc_now = datetime.now(timezone.utc)
        local_time_before = utc_now.astimezone(timezone(timedelta(hours=7)))  # Convert UTC to WIB (UTC+7)
        print(f"Query started at (WIB): {local_time_before.strftime('%Y-%m-%d %H:%M:%S')}")

        start_time = time.time()
        query_result_after = spark.sql(queries)
        query_result_after.show()  # Show the query result (optional)
        query_time_after = time.time() - start_time

        utc_now_after = datetime.now(timezone.utc)
        local_time_after = utc_now_after.astimezone(timezone(timedelta(hours=7)))  # Convert UTC to WIB (UTC+7)
        print(f"Query finished at (WIB): {local_time_after.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Query execution time after optimization: {query_time_after} seconds")

        # Compare the number of files and data size before and after optimization
        print("\nSummary of Optimization:")
        
        before_files = before_optimization.select("numFiles").collect()[0][0]
        after_files = after_optimization.select("numFiles").collect()[0][0]
        print(f"Number of files before optimization: {before_files}")
        print(f"Number of files after optimization: {after_files}")

        before_size = before_optimization.select("sizeInBytes").collect()[0][0]
        after_size = after_optimization.select("sizeInBytes").collect()[0][0]
        print(f"Data size before optimization: {before_size / (1024 * 1024):.2f} MB")
        print(f"Data size after optimization: {after_size / (1024 * 1024):.2f} MB")
        
        # Calculate and print the time saved
        time_saved = query_time_before - query_time_after
        print(f"Time saved after optimization: {time_saved:.4f} seconds ({(time_saved / query_time_before) * 100:.2f}% reduction)")


def generate_unique_id(text):
    """
    helper for dataQuality to generated unique id (primary key)
    """
    try:
        hash_object = hashlib.sha256(text.encode())
        unique_id = hash_object.hexdigest()
        return unique_id
    except Exception as e:
        print(f"[ERROR] Failed to generate unique ID: {e}")
        return None
        

class dataQuality:

    def generate_data_quality_report(
            tableDQ,
            source_observation, 
            destination_observation, 
            source_rows, 
            destination_rows, 
            table_name, 
            process,
            etlType,
            dataFormat,
            file_path
        ):
        """
        Generates a data quality report, logs it into a Delta table, 
        and computes the difference between source and destination records.

        Args:
            tableDQ (str): The Delta table where the data quality report will be stored.

            profile.description_set.table['n']
                source_observation (int): Number of observations in the source.
                destination_observation (int): Number of observations in the destination.

            profile.description_set.variables['Id']['n_distinct']
                source_rows (int): Number of distinct rows in the source.
                destination_rows (int): Number of distinct rows in the destination.
                
            table_name (str): The table name being monitored.
            process (str): The current process being executed.

        """
        # Start Spark session if not already active
        spark = SparkSession.builder.appName("Data Quality Table").getOrCreate()


        # Set Spark configurations for Delta Lake optimizations
        spark.conf.set("spark.microsoft.delta.optimizeWrite.enabled", "true")
        spark.conf.set("spark.microsoft.delta.merge.lowShuffle.enabled", "true")
        spark.conf.set("spark.synapse.vegas.useCache", "true")
        spark.conf.set("spark.microsoft.delta.schema.autoMerge.enabled", "true")
        spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

        try:
            # Attempt to get the Delta table if it exists
            deltaTable = DeltaTable.forName(spark, tableDQ)
            print(f"[INFO] '{tableDQ}' is an existing Delta table.")
        except Exception as e:
            print(f"[INFO] '{tableDQ}' does not exist. Creating a new Delta table.")
            # Define the schema
            schema = """
                ID STRING,
                Updated_Date DATE,
                Updated_Timestamp TIMESTAMP,
                Type STRING,
                Process STRING,
                Type_File_Sources STRING,
                File_Sources STRING,
                Table_Name STRING,
                Status_Observation STRING,
                Source_Observation LONG,
                Destination_Observation LONG,
                Difference_Observation LONG,
                Status_Rows STRING,
                Source_Rows LONG,
                Destination_Rows  LONG,
                Difference_Rows LONG
            """
            empty_df = spark.createDataFrame([], schema)
            empty_df.write.format("delta").mode("overwrite").saveAsTable(f'{tableDQ}')
            deltaTable = DeltaTable.forName(spark, tableDQ)
            print(f"[INFO] Created new Delta table '{tableDQ}'.")

        # Get current timestamp in UTC and convert to desired timezone (WIB)
        utc_now = datetime.now(timezone.utc)
        local_time = utc_now.astimezone(timezone(timedelta(hours=7)))  # Convert UTC to WIB (UTC+7)

        today_str = local_time.strftime("%Y%m%d")
        if file_path:
            file_name = file_path.rstrip('/').split('/')[-1].lower().split('.')[0]
            id_str = f"{today_str}_{file_name}_{etlType.lower()}_{process.lower()}"
            masked_id = generate_unique_id(id_str)

        # Calculate status for rows comparison
        if source_rows and destination_rows:
            difference_rows = source_rows - destination_rows
            status_rows = "PASS" if difference_rows == 0 else "FAIL"
        else:
            difference_rows = None
            status_rows = "UNKNOWN"
        
        # Calculate status for observations comparison
        if source_observation and destination_observation:
            difference_observation = source_observation - destination_observation
            status_observation = "PASS" if difference_observation == 0 else "FAIL"
        else:
            difference_observation = None
            status_observation = "UNKNOWN"


        # Create the Data Quality record
        dq_data = {
            "ID": masked_id,
            "Updated_Date": local_time.strftime("%Y-%m-%d"),
            "Updated_Timestamp": local_time.strftime('%Y-%m-%d %H:%M:%S'),
            "Type": etlType.lower() if etlType else "unknown",
            "Process": process.lower() if process else "unknown",
            "Type_File_Sources": dataFormat.lower() if dataFormat else "unknown",
            "File_Sources": file_path.lower() if file_path else "unknown",
            "Table_Name": table_name,
            "Status_Observation": status_observation,
            "Source_Observation": source_observation,
            "Destination_Observation": destination_observation,
            "Difference_Observation": difference_observation,
            "Status_Rows": status_rows,
            "Source_Rows": source_rows,
            "Destination_Rows": destination_rows,
            "Difference_Rows": difference_rows
        }

        # Convert to DataFrame
        dqr = spark.createDataFrame([dq_data])
        print(f"[INFO] Created DataFrame for DQ results: {dq_data}.")

        # Merge the DQ results into the Delta table
        try:

            deltaTable.alias("tgt").merge(
                dqr.alias("src"),
                "tgt.ID = src.ID"
            ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
            print("[INFO] Successfully merged DQ results into the Delta table.")

        except Exception as e:
            print(f"[ERROR] Failed to merge DQ results into Delta table: {e}")

        # Track update and insert data
        try:
            last_operation = deltaTable.history(1).select("operationMetrics").collect()[0]
            update_count = last_operation["operationMetrics"].get("numTargetRowsUpdated", 0)
            insert_count = last_operation["operationMetrics"].get("numTargetRowsInserted", 0)
            print(f"Number of rows updated: {update_count}")
            print(f"Number of rows inserted: {insert_count}")

        except Exception as e:
            print(f"[ERROR] Failed to retrieve operation metrics: {e}")
