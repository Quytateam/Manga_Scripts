from pymongo import MongoClient

# MongoDB connection URI
uri = "mongodb://localhost:27017/graduation-project"

# Name of the database and collection
database_name = "graduation-project"
def connect_to_mongodb():
    try:
        client = MongoClient(uri)
        db = client.get_database(database_name)
        return db
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        return None