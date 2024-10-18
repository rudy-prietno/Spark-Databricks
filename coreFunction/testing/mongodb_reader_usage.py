from pymongo.errors import InvalidOperation
from bson import ObjectId
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

    # Step 2: check list database
    databases = data_reader.list_databases()
    print(databases)

    # Step 3: check list collection
    collections = data_reader.list_collections(db_name='test')
    print(collections)

    # Step 3: Find one document based on a filter (e.g., product_name is 'Laptop A')
    print("Find one document:")
    # product = data_reader.find_by_id('67118a280d241ba5e276ef7b')
    product = data_reader.find_one({'product_id': 2})
    print(product)


    # Define the date range for the 'updated' field
    start_date = datetime(2024, 10, 17, 0, 0, 0)
    end_date = datetime(2024, 10, 17, 23, 59, 59)

    print("\nFind all documents with projection:")
    # Query documents where 'updated' is between the start_date and end_date, returning all fields
    for doc in data_reader.find_all({"updated": {"$gte": start_date, "$lte": end_date}}):
        print(doc)

    # Query documents where 'updated' is between the start_date and end_date, and 'product_id' is 5
    print("\nFind all documents with projection:")
    query = {
        "updated": {"$gte": start_date, "$lte": end_date},
        "product_id": 5
    }

    # Perform the query and return all fields
    for doc in data_reader.find_all(query):
        print(doc)

    print("\nCount documents:")
    count = data_reader.count_documents()
    print(f"Total documents: {count}")

    print("\nFind with optional filters:")
    start_date = datetime(2024, 10, 17, 0, 0, 0)
    end_date = datetime(2024, 10, 17, 23, 59, 59)
    filter_criteria = {
        "product_id": 5,
        "updated": {"$gte": start_date, "$lte": end_date}
    }
    for doc in data_reader.find_with_optional_filters(filter_criteria):
        print(doc)

except InvalidOperation as e:
    print(f"An error occurred: {e}")

finally:
    # Step 4: Close the MongoDB connection after usage
    if data_reader:
        data_reader.close()
