# Databricks notebook source
import os
import pandas as pd

import psycopg2

import mysql.connector
from mysql.connector import Error

import pymssql

import threading
from bson import ObjectId
import pymongo
from pymongo.errors import ConnectionFailure

from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType
from pyspark.sql import DataFrame

from delta.tables import DeltaTable
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


class PostgreSQLConnectionError(Exception):
    """Custom exception for PostgreSQL connection errors."""
    pass

class PostgreSQLConnector:
    """
    Singleton class to handle PostgreSQL connections.
    Ensures only one instance of the connection throughout the application lifecycle.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(PostgreSQLConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self, host, db, user, password, port, sslmode=None):
        """
        Initializes the PostgreSQLConnector with the necessary credentials and SSL mode.
        """
        self.conn = None
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port
        self.sslmode = sslmode  # Use SSL mode for secure connection
        self._validate_credentials()
        self._create_connection()

    def _validate_credentials(self):
        """
        Validates the necessary PostgreSQL credentials and specifies which ones are missing.
        Raises:
            PostgreSQLConnectionError: If any required credentials are missing, with specific message.
        """
        missing_credentials = []
        
        if not self.host:
            missing_credentials.append("host")
        if not self.db:
            missing_credentials.append("db")
        if not self.user:
            missing_credentials.append("user")
        if not self.password:
            missing_credentials.append("password")
        if not self.port:
            missing_credentials.append("port")
        
        if missing_credentials:
            raise PostgreSQLConnectionError(f"Missing PostgreSQL credentials: {', '.join(missing_credentials)}")

    def _create_connection(self):
        """
        Establishes the PostgreSQL connection using psycopg2.
        Includes SSL mode for secure communication.
        """
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.db,
                user=self.user,
                password=self.password,
                port=self.port,
                sslmode=self.sslmode  # Enforce secure SSL connections
            )
        except psycopg2.Error as e:
            raise PostgreSQLConnectionError(f"Error connecting to PostgreSQL: {str(e)}")

    def get_connection(self):
        """Returns the active PostgreSQL connection."""
        return self.conn

    def get_connection_id(self):
        """Returns the PostgreSQL connection's backend process ID."""
        if self.conn.closed == 0:  # Check if the connection is open
            with self.conn.cursor() as cursor:
                cursor.execute("SELECT pg_backend_pid()")
                connection_id = cursor.fetchone()[0]
            return connection_id
        else:
            raise PostgreSQLConnectionError("Connection is closed")

    def close_connection(self):
        """Closes the PostgreSQL connection."""
        if self.conn:
            self.conn.close()
            return True
        return False
        

class MySQLConnectionError(Exception):
    """Custom exception for MySQL connection errors."""
    pass

class MySQLConnector:
    """
    Singleton class to handle MySQL connections.
    Ensures only one instance of the connection throughout the application lifecycle.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(MySQLConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self, host, db, user, password, port=3306, ssl_ca=None, ssl_cert=None, ssl_key=None):
        """
        Initializes the MySQLConnector with the necessary credentials and optional SSL configuration.
        """
        self.conn = None
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port
        self.ssl_ca = ssl_ca
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self._validate_credentials()
        self._create_connection()

    def _validate_credentials(self):
        """
        Validates the necessary MySQL credentials.
        Raises:
            MySQLConnectionError: If any required credentials are missing.
        """
        missing_credentials = []
        
        if not self.host:
            missing_credentials.append("host")
        if not self.db:
            missing_credentials.append("db")
        if not self.user:
            missing_credentials.append("user")
        if not self.password:
            missing_credentials.append("password")
        if not self.port:
            missing_credentials.append("port")
        
        if missing_credentials:
            raise MySQLConnectionError(f"Missing MySQL credentials: {', '.join(missing_credentials)}")

    def _create_connection(self):
        """
        Establishes the MySQL connection using mysql.connector, with optional SSL configuration.
        """
        try:
            connection_params = {
                'host': self.host,
                'database': self.db,
                'user': self.user,
                'password': self.password,
                'port': self.port
            }

            # Add SSL parameters if provided
            if self.ssl_ca and self.ssl_cert and self.ssl_key:
                connection_params['ssl_ca'] = self.ssl_ca
                connection_params['ssl_cert'] = self.ssl_cert
                connection_params['ssl_key'] = self.ssl_key

            self.conn = mysql.connector.connect(**connection_params)

            if self.conn.is_connected():
                print("Successfully connected to MySQL")
        except Error as e:
            raise MySQLConnectionError(f"Error connecting to MySQL: {str(e)}")

    def get_connection(self):
        """Returns the active MySQL connection."""
        return self.conn

    def get_connection_id(self):
        """Returns the MySQL connection's backend process ID."""
        if self.conn.is_connected():
            cursor = self.conn.cursor()
            cursor.execute("SELECT CONNECTION_ID()")
            connection_id = cursor.fetchone()[0]
            cursor.close()
            return connection_id
        else:
            raise MySQLConnectionError("Connection is closed")

    def close_connection(self):
        """Closes the MySQL connection."""
        if self.conn and self.conn.is_connected():
            self.conn.close()
            print("MySQL connection closed")
            return True
        return False
        

class SQLServerConnectionError(Exception):
    """Custom exception for SQL Server connection errors."""
    pass

class SQLServerConnector:
    """
    Singleton class to handle SQL Server connections.
    Ensures only one instance of the connection throughout the application lifecycle.
    """
    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(SQLServerConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self, host, db, user, password, port=1433, ssl_ca=None, ssl_cert=None, ssl_key=None):
        """
        Initializes the SQLServerConnector with the necessary credentials and optional SSL configuration.
        """
        self.conn = None
        self.host = host
        self.db = db
        self.user = user
        self.password = password
        self.port = port
        self.ssl_ca = ssl_ca
        self.ssl_cert = ssl_cert
        self.ssl_key = ssl_key
        self._validate_credentials()
        self._create_connection()

    def _validate_credentials(self):
        """
        Validates the necessary SQL Server credentials.
        Raises:
            SQLServerConnectionError: If any required credentials are missing.
        """
        missing_credentials = []
        
        if not self.host:
            missing_credentials.append("host")
        if not self.db:
            missing_credentials.append("db")
        if not self.user:
            missing_credentials.append("user")
        if not self.password:
            missing_credentials.append("password")
        if not self.port:
            missing_credentials.append("port")
        
        if missing_credentials:
            raise SQLServerConnectionError(f"Missing SQL Server credentials: {', '.join(missing_credentials)}")

    def _create_connection(self):
        """
        Establishes the SQL Server connection using pymssql, with optional SSL configuration.
        """
        try:
            connection_params = {
                'server': self.host,
                'database': self.db,
                'user': self.user,
                'password': self.password,
                'port': self.port
            }

            if self.ssl_ca:
                connection_params['ssl_ca'] = self.ssl_ca
            if self.ssl_cert:
                connection_params['ssl_cert'] = self.ssl_cert
            if self.ssl_key:
                connection_params['ssl_key'] = self.ssl_key

            self.conn = pymssql.connect(**connection_params)
            print("Successfully connected to SQL Server")
        except pymssql.Error as e:
            raise SQLServerConnectionError(f"Error connecting to SQL Server: {str(e)}")

    def get_connection(self):
        """Returns the active SQL Server connection."""
        return self.conn

    def get_connection_id(self):
        """Returns the SQL Server connection's session ID."""
        with self.conn.cursor() as cursor:
            cursor.execute("SELECT @@SPID")
            connection_id = cursor.fetchone()[0]
        return connection_id

    def close_connection(self):
        """Closes the SQL Server connection."""
        if self.conn:
            self.conn.close()
            print("SQL Server connection closed")
            return True
        return False


# MongoDBConnector class
class MongoDBConnectionError(Exception):
    """Custom exception for MongoDB connection errors."""
    pass

class MongoDBConnector:
    _instance = None
    _lock = threading.Lock()  # Lock object to ensure thread safety

    def __new__(cls, *args, **kwargs):
        with cls._lock:  # Ensure that only one thread at a time can create the instance
            if cls._instance is None:
                cls._instance = super(MongoDBConnector, cls).__new__(cls)
        return cls._instance

    def __init__(self, host, port, user, password, db_name, auth_source=None, ssl=False, ssl_ca=None):
        # Prevent reinitialization
        if not hasattr(self, '_initialized'):
            self.host = host
            self.port = port
            self.user = user
            self.password = password
            self.db_name = db_name
            self.auth_source = auth_source
            self.ssl = ssl
            self.ssl_ca = ssl_ca
            self.client = None
            self.db = None
            self._validate_credentials()
            self._create_connection()
            self._initialized = True

    def _validate_credentials(self):
        """Validates the necessary MongoDB credentials."""
        missing_credentials = []
        if not self.host:
            missing_credentials.append("host")
        if not self.db_name:
            missing_credentials.append("db_name")
        if not self.user:
            missing_credentials.append("user")
        if not self.password:
            missing_credentials.append("password")
        if not self.port:
            missing_credentials.append("port")

        if missing_credentials:
            raise MongoDBConnectionError(f"Missing MongoDB credentials: {', '.join(missing_credentials)}")

    def _create_connection(self):
        """Establishes the MongoDB connection using pymongo."""
        try:
            # Build the MongoDB URI
            connection_uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"

            # Add authSource if provided
            if self.auth_source:
                connection_uri += f"?authSource={self.auth_source}"

            # Connection parameters
            connection_params = {
                'host': connection_uri,
                'ssl': self.ssl
            }

            if self.ssl_ca:
                connection_params['tlsCAFile'] = self.ssl_ca

            # Create the MongoDB client
            self.client = pymongo.MongoClient(**connection_params)
            self.db = self.client[self.db_name]

            # Test the connection
            self.client.admin.command('ping')
            print("Successfully connected to MongoDB")
        except ConnectionFailure as e:
            raise MongoDBConnectionError(f"Error connecting to MongoDB: {str(e)}")

    def get_connection(self):
        """Returns the active MongoDB connection."""
        if self.client is None:
            raise MongoDBConnectionError("MongoDB connection is not established.")
        return self.db

    def close_connection(self):
        """Closes the MongoDB connection."""
        if self.client:
            self.client.close()
            print("MongoDB connection closed")
            return True
        return False

    def list_databases(self):
        """Lists all databases in the MongoDB instance."""
        return self.client.list_database_names()

    def list_collections(self, db_name=None):
        """Lists all collections in the specified database. If no database is provided, it uses the current database."""
        db = self.client[db_name] if db_name else self.db
        return db.list_collection_names()


class MongoDBDataReader:
    def __init__(self, db_connector: MongoDBConnector, collection_name: str):
        """Initializes the MongoDBDataReader with a MongoDBConnector instance and the collection name."""
        self.db_connector = db_connector
        self.collection_name = collection_name
        self.collection = self._get_collection()

    def _get_collection(self):
        """Fetches the specified collection from the database."""
        if not self.db_connector or not self.collection_name:
            raise ValueError("Invalid database connector or collection name.")
        return self.db_connector.get_connection()[self.collection_name]

    def find_one(self, query: dict = None, projection: dict = None):
        """
        Finds a single document in the collection based on the provided query.
        Supports projection to include/exclude fields.
        """
        if query is None:
            query = {}
        return self.collection.find_one(query, projection)

    def find_all(self, query: dict = None, projection: dict = None, limit: int = 0):
        """
        Finds multiple documents in the collection based on the provided query.
        Supports projection to include/exclude fields.
        """
        if query is None:
            query = {}
        return self.collection.find(query, projection).limit(limit)

    def count_documents(self, query: dict = None):
        """Returns the count of documents in the collection based on the provided query."""
        if query is None:
            query = {}
        return self.collection.count_documents(query)

    def find_by_id(self, document_id, projection: dict = None):
        """
        Finds a single document by its _id.
        Supports projection to include/exclude fields.
        """
        query = {"_id": ObjectId(document_id)}
        return self.collection.find_one(query, projection)

    def find_with_optional_filters(self, filter_criteria: dict, projection: dict = None, limit: int = 0):
        """
        Finds documents based on optional filter criteria.
        Any filter with a value of None will be ignored.
        Supports projection to include/exclude fields.
        """
        query = {key: value for key, value in filter_criteria.items() if value is not None}
        return self.collection.find(query, projection).limit(limit)

    def list_databases(self):
        """Lists all databases in the MongoDB instance."""
        return self.db_connector.list_databases()

    def list_collections(self, db_name=None):
        """Lists all collections in the specified database. If no database is provided, it uses the current database."""
        return self.db_connector.list_collections(db_name)

    def close(self):
        """Closes the MongoDB connection via the MongoDBConnector."""
        return self.db_connector.close_connection()


class MongoToSpark:
    def __init__(self, spark_session):
        """
        Initializes the MongoToSpark class.
        
        :param spark_session: An active Spark session.
        """
        self.spark = spark_session

    def query_to_rdd(self, documents):
        """
        Converts MongoDB query results to a Spark RDD.
        
        :param documents: List of MongoDB documents.
        :return: A Spark RDD containing the documents from MongoDB.
        """
        # Preprocess documents to convert _id to string
        processed_docs = []
        for doc in documents:
            if '_id' in doc:
                doc['_id'] = str(doc['_id'])  # Convert ObjectId to string
            processed_docs.append(doc)

        # Convert documents to RDD
        rdd = self.spark.sparkContext.parallelize(processed_docs)
        return rdd

    def rdd_to_dataframe(self, rdd, schema: StructType = None):
        """
        Converts an RDD to a Spark DataFrame with an optional schema.
        
        :param rdd: The Spark RDD.
        :param schema: Optional schema for the DataFrame (StructType). If not provided, Spark will infer the schema.
        :return: A PySpark DataFrame.
        """
        if schema:
            # Create DataFrame with a predefined schema
            df = self.spark.createDataFrame(rdd.map(lambda doc: Row(**doc)), schema)
        else:
            # Infer schema automatically
            df = self.spark.createDataFrame(rdd.map(lambda doc: Row(**doc)))
        
        return df
        

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

    @staticmethod
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


    @staticmethod
    def DeltaTablesPartition(queries, tableName, partitionKeys, orderKeys, partitionColumns, partitionColumnSecondary):
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
        spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

        # Timestamp handling settings
        spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.int96RebaseModeInWrite", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

        # Execute the provided SQL query to get the source DataFrame
        rsDf = spark.sql(queries)

        # Create a WindowSpec with partition and order columns
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
            print(f"{row_count} rows written to {tableName}.")
        

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
