import os
import polars as pl
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger


class DBConnector:
    def __init__(self):
        self.engine = None
        self.SessionLocal = None
        self.mongo_client = None
        self.mongo_db = None

    def connect_sqlalchemy(self, db_url, connect_args=None):
        if connect_args is None:
            connect_args = {}
        try:
            self.engine = create_engine(db_url, connect_args=connect_args)
            self.SessionLocal = sessionmaker(autoflush=False, bind=self.engine)
        except Exception as e:
            logger.error(f"SQLAlchemy connection error: {e}")

    def connect_mongodb(self, uri, db_name):
        try:
            self.mongo_client = MongoClient(uri, server_api=ServerApi("1"))
            self.mongo_db = self.mongo_client[db_name]
            self.mongo_client.admin.command("ping")
            logger.info("Successfully connected to MongoDB")
        except Exception as e:
            logger.error(f"MongoDB connection error: {e}")

    def initialize_db(self, connection_type):
        if connection_type == "PLANETSCALE":
            db_url = self.build_mysql_url()
            ssl_args = {"ssl": {"ca": "/etc/ssl/certs/ca-certificates.crt"}}
            self.connect_sqlalchemy(db_url, ssl_args)

        elif connection_type == "LOCAL":
            db_url = self.build_postgres_url()
            self.connect_sqlalchemy(db_url)

        elif connection_type == "MONGO":
            mongo_uri = os.getenv("MONGO_URI")
            mongo_db_name = os.getenv("MONGO_DB_NAME")
            self.connect_mongodb(mongo_uri, mongo_db_name)

        else:
            raise ValueError("Invalid connection type specified")

    @staticmethod
    def build_mysql_url():
        return f"mysql+pymysql://{os.getenv('DB_USERNAME')}:{os.getenv('DB_PASSWORD')}@{os.getenv('DB_HOST')}/{os.getenv('DB_NAME')}"

    @staticmethod
    def build_postgres_url():
        return f"postgresql://{os.getenv('DB_USERNAME_LOCAL')}:{os.getenv('DB_PASSWORD_LOCAL')}@{os.getenv('DB_HOST_LOCAL')}/{os.getenv('DB_NAME_LOCAL')}"

    def write_to_mongo(self, collection_name, df):
        try:
            records = df.to_dicts()
            collection = self.mongo_db[collection_name]
            result = collection.insert_many(records)
            logger.info(f"Inserted document IDs: {result.inserted_ids}")
        except Exception as e:
            logger.error(f"Failed to write to MongoDB. Error: {e}")

    def read_from_mongo(self, collection_name, query=None):
        if query is None:
            query = {}
        try:
            collection = self.mongo_db[collection_name]
            data = list(collection.find(query))
            return pl.DataFrame(data)
        except Exception as e:
            logger.error(f"Failed to read from MongoDB. Error: {e}")
            return pl.DataFrame()

    def create_collection(self, db_uri, db_name, collection_name, validator=None):
        if validator is None:
            validator = {}
        try:
            client = MongoClient(db_uri)
            db = client[db_name]
            db.create_collection(collection_name, validator=validator)
            db.command("collMod", collection_name, validator=validator)
        except Exception as e:
            logger.error(f"Error creating collection: {e}")


# # Initialize DBConnector
# db = DBConnector()
# db.initialize_db("MONGO")
# db.create_collection(
#     os.getenv("MONGO_URI"), os.getenv("MONGO_DB_NAME"), "your_collection_name"
# )
