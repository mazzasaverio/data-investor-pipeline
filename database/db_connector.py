# db_connector.py

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os
import pandas as pd


class DBConnector:
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self.mongo_client = None
        self.mongo_db = None

    def connect_sqlalchemy(self, db_url, connect_args={}):
        try:
            self.engine = create_engine(db_url, connect_args=connect_args)
            self.SessionLocal = sessionmaker(autoflush=False, bind=self.engine)

        except Exception as e:
            # Handle exceptions and logging
            pass

    def connect_mongodb(self, uri, db_name):
        try:
            self.mongo_client = MongoClient(uri, server_api=ServerApi("1"))
            self.mongo_db = self.mongo_client[db_name]

            self.mongo_client.admin.command("ping")
            print("Pinged your deployment. You successfully connected to MongoDB!")

        except Exception as e:
            # Handle exceptions and logging
            pass

    def initialize_db(self, connection_type):
        if connection_type == "PLANETSCALE":
            db_url = f"mysql+pymysql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"
            ssl_args = {"ssl": {"ca": "/etc/ssl/certs/ca-certificates.crt"}}

            return self.connect_sqlalchemy(db_url, ssl_args)

        elif connection_type == "LOCAL":
            db_url = f"postgresql://{os.getenv('DB_USERNAME_LOCAL')}:{os.getenv('DB_PASSWORD_LOCAL')}@{os.getenv('DB_HOST_LOCAL')}/{os.getenv('DB_NAME_LOCAL')}"

            return self.connect_sqlalchemy(db_url)

        elif connection_type == "MONGO":
            mongo_uri = os.getenv("MONGO_URI")
            mongo_db_name = os.getenv("MONGO_DB_NAME")
            return self.connect_mongodb(mongo_uri, mongo_db_name)

        else:
            raise ValueError("Invalid connection type specified")

    def write_to_mongo(self, collection_name, df):
        try:
            records = df.to_dict(orient="records")

            collection = self.mongo_client[collection_name]
            result = collection.insert_many(records)
            print("Inserted document IDs:", result.inserted_ids)
        except Exception as e:
            print(f"Failed to write to MongoDB. Error: {e}")

    def read_from_mongo(self, collection_name, query={}):
        try:
            collection = self.mongo_client[collection_name]

            data = pd.DataFrame(list(collection.find({})))
            return data
        except Exception as e:
            # Handle exceptions and logging
            return pd.DataFrame()
