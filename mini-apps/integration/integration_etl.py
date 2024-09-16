# Move the file to the correct path
dbutils.fs.cp("dbfs:/FileStore/tables/coreFunction.py", "file:/databricks/driver/coreFunction.py")

# Add the directory to the system path
import sys
sys.path.append("/databricks/driver/")

# Now, import the coreFunction module
import coreFunction
import pandas as pd
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, TimestampType
from pyspark.sql import functions as F

# Copy the file from DBFS to the local file system
dbutils.fs.cp(
    "dbfs:/FileStore/tables/Example.xlsx", 
    "file:/tmp/Example.xlsx"
    )

file_path = "/tmp/Example.xlsx"
sheet_name = "Sheet1" 

# Instantiate the DataReader class
readerSource = coreFunction.DataReader(file_path, sheet_name)
dataSource = readerSource.readExcell()

# Rename column to support naming regulation on delta pattern
dataSource = dataSource.rename(columns={
    'Account No': 'AccountNo',
    'DATE':'Date',
    'TRANSACTION DETAILS': 'TransactionDetails',
    'CHQ.NO.': 'CHQNo',
    'VALUE DATE': 'ValueDate',
    'WITHDRAWAL AMT':'WithdrawalAMT',
    'DEPOSIT AMT':'DepositAMT', 	
    'BALANCE AMT':'BalanceAMT'
})

# Remove the extra apostrophe (') from the 'Account No' column
dataSource['AccountNo'] = dataSource['AccountNo'].str.replace("'", "")

# change type data
dataSource['Date'] = pd.to_datetime(dataSource['Date'], errors='coerce')
dataSource['ValueDate'] = pd.to_datetime(dataSource['ValueDate'], errors='coerce') 
dataSource['TransactionDetails'] = dataSource['TransactionDetails'].astype(str)
dataSource['AccountNo'] = dataSource['AccountNo'].astype(str)
dataSource['CHQNo'] = dataSource['CHQNo'].astype(str)

dataSource['WithdrawalAMT'] = dataSource['WithdrawalAMT'].apply(coreFunction.convert_to_decimal)
dataSource['DepositAMT'] = dataSource['DepositAMT'].apply(coreFunction.convert_to_decimal)
dataSource['BalanceAMT'] = dataSource['BalanceAMT'].apply(coreFunction.convert_to_decimal)

# Drop the column with the name '.'
dataSource_cleaned = dataSource.drop(columns=['.'])


# Define structure schema for spark DataFrame
schema = StructType([
    StructField("AccountNo", StringType(), True),
    StructField("Date", DateType(), True),
    StructField("TransactionDetails", StringType(), True),
    StructField("CHQNo", StringType(), True),
    StructField("ValueDate", DateType(), True),
    StructField("WithdrawalAMT", DecimalType(18, 2), True),
    StructField("DepositAMT", DecimalType(18, 2), True),
    StructField("BalanceAMT", DecimalType(18, 2), True)
])

# Convert from pandas dataframe to spark
spark_df = spark.createDataFrame(dataSource_cleaned, schema=schema)

# Convert UTC to Asia/Jakarta
spark_df = spark_df.withColumn("IngestionTime", F.current_timestamp().cast(TimestampType()))
spark_df = spark_df.withColumn("IngestionTime", F.from_utc_timestamp(F.col("IngestionTime"), "Asia/Jakarta"))

# Ingestion data
tableName = "bronze_data.transactions_raw"
coreFunction.DataIngestion.DeltaTables(tableName, spark_df)


# Sources
profiler_xlsx = coreFunction.DataProfiling(titleProfile="Profiling Report XLSX")

# if the time is not provided, you can fill it with '00:00:00'
spark_dfProfile = spark_df.withColumn("Date", F.coalesce(F.col("Date"), F.lit("00:00:00").cast(TimestampType())))
spark_dfProfile = spark_dfProfile.withColumn("ValueDate", F.coalesce(F.col("ValueDate"), F.lit("00:00:00").cast(TimestampType())))

# Convert DecimalType columns to float for profiling or operations that do not support DecimalType
spark_dfProfile = spark_dfProfile.withColumn("WithdrawalAMT", F.col("WithdrawalAMT").cast("float"))
spark_dfProfile = spark_dfProfile.withColumn("DepositAMT", F.col("DepositAMT").cast("float"))
spark_dfProfile = spark_dfProfile.withColumn("BalanceAMT", F.col("BalanceAMT").cast("float"))

# Now convert to pandas DataFrame for profiling
pandas_df = spark_dfProfile.toPandas()

profile_source =profiler_xlsx.profile(pandas_df)

source_observation= profile_source.description_set.table['n']
source_rows= profile_source.description_set.variables['AccountNo']['n_distinct']

# Destination
bronze_df = spark.read.format("delta").table("bronze_data.transactions_raw").select("AccountNo")
bronze_df_profile = bronze_df.toPandas()

profiler_bronze = coreFunction.DataProfiling(titleProfile="Profiling Report Bronze")
profiler_destination =profiler_bronze.profile(bronze_df_profile)

destination_observation= profiler_destination.description_set.table['n']
destination_rows= profiler_destination.description_set.variables['AccountNo']['n_distinct']

coreFunction.dataQuality.generate_data_quality_report(
            tableDQ= 'quality_data.quality_monitoring',
            source_observation= source_observation, 
            destination_observation= destination_observation, 
            source_rows= source_rows, 
            destination_rows= destination_rows, 
            table_name= 'transactions_raw',
            process= 'xlsx_to_bronze',
            etlType= 'integration',
            dataFormat= 'xlsx',
            file_path= '/tmp/_Confidential__Mekari___Data_Engineer_Senior.xlsx'
        )
