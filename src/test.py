from mongo import MongoService, MongoReference
from login import HOST, PORT, USER

if __name__=="__main__":
    mongo_service = MongoService(HOST, PORT, **USER)

    # Type tests
    database_name = "test_database"
    database = mongo_service.database(database_name)
    collection_list = database.list()
    print(f"Database: {database.name} {type(database.database)} {collection_list}")

    collection = database.collection("test_collection")
    print(f"Collection '{collection.name}' {type(collection.collection)} {collection_list}")
