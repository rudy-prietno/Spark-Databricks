# Move the file to the correct path
dbutils.fs.cp("dbfs:/FileStore/tables/coreFunction.py", "file:/databricks/driver/coreFunction.py")

# Add the directory to the system path
import sys
sys.path.append("/databricks/driver/")

# Now, import the coreFunction module
import coreFunction
from pyspark.sql import functions as F

# Generated Unique ID for make a Primary Key
# Load the bronze Delta table into a DataFrame
df_bronze = spark.read.format("delta").table("bronze_data.transactions_raw")

# Adjust the unique ID generation by considering more columns for uniqueness
df_bronze_preparation = df_bronze.withColumn(
    "Id", 
    F.sha2(F.concat(
        F.col("AccountNo"),
        F.col("Date"),
        F.col("TransactionDetails"),
        F.coalesce(F.col("CHQNo").cast("string"), F.lit('')),
        F.coalesce(F.col("WithdrawalAMT").cast("string"), F.lit('')),
        F.coalesce(F.col("DepositAMT").cast("string"), F.lit('')),
        F.coalesce(F.col("BalanceAMT").cast("string"), F.lit(''))
    ), 256)
)


# SQL query to select data from your DataFrame
df_bronze_preparation.createOrReplaceTempView("silver_data_table")

# Define partition and order keys to make sure data is unique
partitionKeys = ['Id']
orderKeys = ['Date', 'IngestionTime']

# Define primary and secondary partition columns to build a partition delta table
partitionColumns = 'Date'
partitionColumnSecondary = 'ValueDate'

queries = """
SELECT * FROM silver_data_table
"""

# Use the DeltaTablesPartition function, enabling schema merge
coreFunction.DataIngestion.DeltaTablesPartition(
    queries=queries,
    tableName="silver_data.transactions_clean",  # Delta table name
    partitionKeys=partitionKeys,
    orderKeys=orderKeys,
    partitionColumns=partitionColumns,
    partitionColumnSecondary=partitionColumnSecondary
)

# Source
bronze_df_source = spark.read.format("delta").table("bronze_data.transactions_raw")

df_bronze_preparation = bronze_df_source.withColumn(
    "Id", 
    F.sha2(F.concat(
        F.col("AccountNo"),
        F.col("Date"),
        F.col("TransactionDetails"),
        F.coalesce(F.col("CHQNo").cast("string"), F.lit('')),
        F.coalesce(F.col("WithdrawalAMT").cast("string"), F.lit('')),
        F.coalesce(F.col("DepositAMT").cast("string"), F.lit('')),
        F.coalesce(F.col("BalanceAMT").cast("string"), F.lit(''))
    ), 256)
)

bronze_df = df_bronze_preparation.select("Id")

bronze_df_profile = bronze_df.toPandas()

profiler_bronze = coreFunction.DataProfiling(titleProfile="Profiling Report Bronze")
profiler_sources =profiler_bronze.profile(bronze_df_profile)

source_observation= profiler_sources.description_set.table['n']
source_rows= profiler_sources.description_set.variables['Id']['n_distinct']


# Destination
silver_df = spark.read.format("delta").table("silver_data.transactions_clean").select("Id")
silver_df_profile = silver_df.toPandas()

profiler_silver = coreFunction.DataProfiling(titleProfile="Profiling Report Silver")
profiler_destination =profiler_silver.profile(silver_df_profile)

destination_observation= profiler_destination.description_set.table['n']
destination_rows= profiler_destination.description_set.variables['Id']['n_distinct']

coreFunction.dataQuality.generate_data_quality_report(
            tableDQ= 'quality_data.quality_monitoring',
            source_observation= source_observation, 
            destination_observation= destination_observation, 
            source_rows= source_rows, 
            destination_rows= destination_rows, 
            table_name= 'transactions_clean',
            process= 'bronze_to_silver',
            etlType= 'platform',
            dataFormat= 'delta',
            file_path= 'dbfs:/user/hive/warehouse/bronze_data.db/transactions_raw'
        )
