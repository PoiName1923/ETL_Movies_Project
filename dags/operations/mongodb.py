from pymongo import MongoClient

from contextlib import contextmanager
import pandas as pd

@contextmanager
def connect_mongodb(username : str, password : str, host : str ,port : str):
    uri = f"mongodb://{username}:{password}@{host}:{port}"
    client  = None
    try:
        print("Connecting to MongoDB")
        client = MongoClient(uri)
        print("Connect to MongoDB")
        yield client
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if client:
            client.close()
            print("Stop Connect to MongoDB")
        else:
            print("Can't generate MongoDB Client")
        
class MongoDB_Operation:
    """Init"""
    def __init__(self, client):
        self.client = client

    """Check if a database exists."""
    def check_database_exists(self, db_name):
        return db_name in self.client.list_database_names()
    
    """Check if a collection exists in a database."""
    def check_collection_exists(self, db_name, collection_name):
        return collection_name in self.client[db_name].list_collection_names()
    
    """Create a database if it does not exist."""
    def create_database(self, db_name):
        """Create a database if it does not exist."""
        if self.check_database_exists(db_name):
            print(f"Database '{db_name}' already exists!")
            return self.client[db_name]
        else:
            self.client[db_name].command("ping")
            print(f"Database '{db_name}' has been created successfully.")
            return self.client[db_name]
        
    """Create a collection if it does not exist."""     
    def create_collection(self, db_name, collection_name):
        db = self.create_database(db_name)
        if self.check_collection_exists(db_name, collection_name):
            print(f"Collection '{collection_name}' already exists!")
            return db[collection_name]
        else:
            db.create_collection(collection_name)
            print(f"Collection '{collection_name}' has been created successfully.")
            return db[collection_name]
        
    """Find data in a collection."""
    def find_data(self, db_name, collection_name, query=None):
        if not isinstance(query, dict):
            raise TypeError("Query must be a dictionary")
        if not self.check_database_exists(db_name):
            print(f"Database '{db_name}' does not exist!")
        if not self.check_collection_exists(db_name, collection_name):
            print(f"Collection '{collection_name}' does not exist!")

        query = query or {}
        data = self.client[db_name][collection_name].find(query)
        return pd.DataFrame(data)
    
    """Insert multiple records into a collection."""
    def insert_many(self, db_name, collection_name, data : pd.DataFrame):
        coll = self.create_collection(db_name=db_name, collection_name=collection_name)
        data_dict = data.to_dict(orient='records')
        if data_dict:
            coll.insert_many(data_dict)
            print("Data has been added successfully.")
        else:
            print("Empty data, nothing to insert.")