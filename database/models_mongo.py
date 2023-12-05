from pymongo import MongoClient


def create_cashflows_collection(db_uri, db_name):
    client = MongoClient(db_uri)
    db = client[db_name]
    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": ["cik", "acceptedDate", "calendarYear", "freeCashFlow"],
            "properties": {
                "cik": {"bsonType": "long"},
                "acceptedDate": {"bsonType": "date"},
                "calendarYear": {"bsonType": "long"},
                "freeCashFlow": {"bsonType": "long"},
            },
        }
    }

    # Drop the existing collection if it's okay to lose the data
    db.drop_collection("cashflows")

    # Recreate the collection with the new validator
    # db.create_collection("cashflows", validator=validator)
    db.create_collection("cashflows", validator={})
    # db.command("collMod", "cashflows", validator=validator)

    db.command("collMod", "cashflows", validator={})

    return db["cashflows"]


def create_profiles_collection(db_uri, db_name):
    client = MongoClient(db_uri)
    db = client[db_name]

    validator = {
        "$jsonSchema": {
            "bsonType": "object",
            "required": [
                "symbol",
                "companyName",
                "cik",
                "exchange",
                "exchangeShortName",
                "industry",
                "sector",
                "country",
                "ipoDate",
                "defaultImage",
                "isEtf",
                "isActivelyTrading",
            ],
            "properties": {
                "symbol": {"bsonType": "string"},
                "companyName": {"bsonType": "string"},
                "cik": {"bsonType": "int"},
                "exchange": {"bsonType": "string"},
                "exchangeShortName": {"bsonType": "string"},
                "industry": {"bsonType": "string"},
                "sector": {"bsonType": "string"},
                "country": {"bsonType": "string"},
                "ipoDate": {"bsonType": "date"},
                "defaultImage": {"bsonType": "bool"},
                "isEtf": {"bsonType": "bool"},
                "isActivelyTrading": {"bsonType": "bool"},
            },
        }
    }

    # Controlla se la collezione esiste già
    if "profiles" not in db.list_collection_names():
        db.create_collection("profiles")
        db.command("collMod", "profiles", validator=validator)
    else:
        print("La collezione 'profiles' esiste già.")

    # Aggiungi il validator dopo aver creato la collezione
    db.command("collMod", "profiles", validator=validator)

    return db["profiles"]
