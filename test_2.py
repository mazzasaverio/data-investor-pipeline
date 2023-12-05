# Import necessary libraries
import os
import pandas as pd
from pymongo import MongoClient
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME")

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[MONGO_DB_NAME]


# Function to create a collection with optional validator
def create_profiles2_collection(db, collection_name, validator=None):
    if collection_name not in db.list_collection_names():
        if validator:
            db.create_collection(collection_name, validator=validator)
        else:
            db.create_collection(collection_name)
    else:
        print(f"Collection '{collection_name}' already exists.")


# Function to insert data into MongoDB
def insert_data_to_mongo(db, collection_name, data_frame):
    collection = db[collection_name]
    records = data_frame.to_dict(orient="records")
    collection.insert_many(records)


# Sample DataFrame
data = {
    "symbol": ["AAPL", "MSFT"],
    "companyName": ["Apple Inc.", "Microsoft Corporation"],
    "cik": [320193, 789019],
    # Add other fields as necessary
}
df = pd.DataFrame(data)

# Create collection and insert data
create_profiles2_collection(db, "profiles2")  # Create collection if it doesn't exist
insert_data_to_mongo(db, "profiles2", df)  # Insert data

# Verify the inserted data
profiles2_collection = db["profiles2"]
documents = profiles2_collection.find({})
for doc in documents:
    print(doc)
