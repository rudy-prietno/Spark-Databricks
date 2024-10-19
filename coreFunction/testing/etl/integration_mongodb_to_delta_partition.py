# Move the file to the correct path
dbutils.fs.cp("dbfs:/FileStore/tables/coreFunction.py", "file:/databricks/driver/coreFunction.py")

# Add the directory to the system path
import sys
sys.path.append("/databricks/driver/")

# Now, import the coreFunction module
import coreFunction
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType
from pymongo.errors import InvalidOperation
from datetime import datetime

try:
    # Step 1: Set up the MongoDBConnector
    db_connector = MongoDBConnector(
        host='host',
        port=27017,
        user='user_mongodb',
        password='password_mongodb',
        db_name='database_mongodb',
        auth_source='user_mongodb',
        ssl=False
    )

    # Initialize MongoDBDataReader with the 'product' collection
    data_reader = MongoDBDataReader(db_connector, 'product')

    print("\nFind with optional filters:")
    start_date = datetime(2024, 10, 17, 0, 0, 0)
    end_date = datetime(2024, 10, 17, 23, 59, 59)
    filter_criteria = {
        "product_id": 5,
        "updated": {"$gte": start_date, "$lte": end_date}
    }

    # Fetch documents from MongoDB
    docs = []
    for doc in data_reader.find_with_optional_filters(filter_criteria):
        docs.append(doc)

    # Create an RDD
    mongo_to_spark = MongoToSpark(spark)
    rdd = mongo_to_spark.query_to_rdd(docs)

    # Define a schema (for flexibility)
    schema = StructType([
        StructField("_id", StringType(), True),
        StructField("product_id", LongType(), True),
        StructField("product_name", StringType(), True),
        StructField("product_line", StringType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("created", TimestampType(), True),
        StructField("updated", TimestampType(), True)
    ])

    # Convert RDD to DataFrame using the custom schema
    df = mongo_to_spark.rdd_to_dataframe(rdd, schema)

    # Step 3: Write the DataFrame to a Temporary Delta Table
    temp_table_name = "temp_product"
    df.write.format("delta").mode("overwrite").saveAsTable(temp_table_name)
    
    # Step 4: Use DataIngestion.DeltaTablesPartition to Partition and Merge Data
    queries = f"SELECT * FROM {temp_table_name}"
    tableName = "partition_product"
    partitionKeys = ["product_id"]
    orderKeys = ["updated"]
    partitionColumns = "updated"
    partitionColumnSecondary = "created"
    
    # Call DeltaTablesPartition function
    DataIngestion.DeltaTablesPartition(
        queries=queries,
        tableName=tableName,
        partitionKeys=partitionKeys,
        orderKeys=orderKeys,
        partitionColumns=partitionColumns,
        partitionColumnSecondary=partitionColumnSecondary
    )
    
    # After the ingestion process, drop the temporary table
    spark.sql(f"DROP TABLE IF EXISTS {temp_table_name}")
    print(f"Temporary Delta table {temp_table_name} has been dropped.")

except InvalidOperation as e:
    print(f"An error occurred: {e}")

finally:
    # Step 4: Close the MongoDB connection after usage
    if data_reader:
        data_reader.close()
