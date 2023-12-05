# run.py

# Import necessary libraries
from datetime import datetime
from dotenv import load_dotenv
import os
from loguru import logger
import sys

# Import custom modules
from database.db_connector import DBConnector

from fetch_data.fetch_equities import (
    fetch_and_store_profiles,
    fetch_and_store_financial_statements,
    fetch_and_store_hist_prices,
    fetch_and_store_market_cap_data,
)
from database.models_mongo import (
    create_profiles_collection,
    create_cashflows_collection,
)


def configure_logger():
    """
    Configure the Loguru logger settings.
    """
    logger.remove()
    logger.add(
        lambda msg: sys.stderr.write(msg),
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {function}:{line} - {message}",
    )
    logger.add(
        "./logs/data_pipeline.log", rotation="1 day", level="INFO", serialize=True
    )


def main():
    load_dotenv()
    configure_logger()

    run_id = datetime.now().strftime("%Y/%m/%d_%H:%M:%S")
    FMP_API_KEY = os.getenv("FMP_SECRET_KEY")
    db_type = os.getenv("DB_TYPE", "LOCAL")

    # Initialize DB Connector based on the type
    print(f"Connecting to DB: {db_type}")
    db_connector = DBConnector()

    db_connector.initialize_db(db_type)

    common_args = {
        "db_connector": db_connector,
        "api_key": FMP_API_KEY,
        "run_id": run_id,
    }

    try:
        logger.bind(run_id=run_id).info("Starting data pipeline")
        db_uri = os.getenv("MONGO_URI")
        db_name = os.getenv("MONGO_DB_NAME")
        create_profiles_collection(db_uri, db_name)

        create_cashflows_collection(db_uri, db_name)

        fetch_and_store_profiles(**common_args)
        fetch_and_store_financial_statements(**common_args)
        # fetch_and_store_hist_prices(**common_args)
        # fetch_and_store_market_cap_data(**common_args)

    except Exception as e:
        logger.bind(run_id=run_id).exception("An error occurred: {}", e)


if __name__ == "__main__":
    main()
