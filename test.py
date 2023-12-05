from database.db_session import initialize_db
import os
import pandas as pd
from database.models_mongo import create_profiles_collection

db_type = os.getenv("DB_TYPE", "LOCAL")
db_uri = os.getenv("MONGO_URI")
db_name = os.getenv("MONGO_DB_NAME")

# Initialize DB Connector based on the type
db_connector = initialize_db(db_type)


profiles_collection = create_profiles_collection(db_uri, db_name)


data = {
    "symbol": ["AAPL", "MSFT"],
    "companyName": ["Apple Inc.", "Microsoft Corporation"],
    "cik": [320193, 789019],
    "exchange": ["NASDAQ", "NASDAQ"],
    "exchangeShortName": ["NASDAQ", "NASDAQ"],
    "industry": ["Consumer Electronics", "Software"],
    "sector": ["Technology", "Technology"],
    "country": ["United States", "United States"],
    "ipoDate": [pd.to_datetime("1980-12-12"), pd.to_datetime("1986-03-13")],
    "defaultImage": [False, False],
    "isEtf": [False, False],
    "isActivelyTrading": [True, True],
}

df = pd.DataFrame(data)


# Convert DataFrame to a list of dictionaries
records = df.to_dict(orient="records")

profiles_collection = db_connector.profiles

# Insert the documents
result = profiles_collection.insert_many(records)
print("Inserted document IDs:", result.inserted_ids)

profiles_collection = db_connector["profiles"]
documents = profiles_collection.find({})
for doc in documents:
    print(doc)
