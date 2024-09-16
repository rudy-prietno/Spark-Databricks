# Move the file to the correct path
dbutils.fs.cp("dbfs:/FileStore/tables/coreFunction.py", "file:/databricks/driver/coreFunction.py")

# Add the directory to the system path
import sys
sys.path.append("/databricks/driver/")

# Now, import the coreFunction module
import coreFunction
from pyspark.sql import functions as F

silver_df = spark.read.format("delta").table("silver_data.transactions_clean")

# Transform: Calculate TotalDeposits, TotalWithdrawals, NetBalance, and LastTransactionDate
transformed_df = silver_df.groupBy("AccountNo").agg(
    F.sum("DepositAMT").alias("TotalDeposits"),
    F.sum("WithdrawalAMT").alias("TotalWithdrawals"),
    F.last("BalanceAMT").alias("NetBalance"),
    F.max("Date").alias("LastTransactionDate")
)

coreFunction.DataIngestion.DeltaTables(
    tableName="gold_data.transactions_datamart", 
    dataFrameSource=transformed_df, 
    primaryKey="AccountNo"
    )

# sources
silver_df_profile = spark.read.format("delta").table("silver_data.transactions_clean")

transformed_df = silver_df_profile.groupBy("AccountNo").agg(
    F.sum("DepositAMT").alias("TotalDeposits"),
    F.sum("WithdrawalAMT").alias("TotalWithdrawals"),
    F.last("BalanceAMT").alias("NetBalance"),
    F.max("Date").alias("LastTransactionDate")
)

silver_df = transformed_df.select("AccountNo")

silver_df_profile = silver_df.toPandas()

profiler_silver = coreFunction.DataProfiling(titleProfile="Profiling Report Silver: Gold")
profiler_sources =profiler_silver.profile(silver_df_profile)

source_observation= profiler_sources.description_set.table['n']
source_rows= profiler_sources.description_set.variables['AccountNo']['n_distinct']

# destination
gold_df_profile = spark.read.format("delta").table("gold_data.transactions_datamart").select("AccountNo")

gold_df_profile = gold_df_profile.toPandas()

profiler_gold = coreFunction.DataProfiling(titleProfile="Profiling Report Gold")
profiler_destination =profiler_gold.profile(gold_df_profile)

destination_observation= profiler_destination.description_set.table['n']
destination_rows= profiler_destination.description_set.variables['AccountNo']['n_distinct']


coreFunction.dataQuality.generate_data_quality_report(
            tableDQ= 'quality_data.quality_monitoring',
            source_observation= source_observation, 
            destination_observation= destination_observation, 
            source_rows= source_rows, 
            destination_rows= destination_rows, 
            table_name= 'transactions_datamart',
            process= 'silver_to_gold',
            etlType= 'datamart',
            dataFormat= 'delta',
            file_path= 'dbfs:/user/hive/warehouse/gold_data.db/transactions_datamart'
        )
