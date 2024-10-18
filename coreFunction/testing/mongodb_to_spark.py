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
    df.show()

except InvalidOperation as e:
    print(f"An error occurred: {e}")

finally:
    # Step 4: Close the MongoDB connection after usage
    if data_reader:
        data_reader.close()
