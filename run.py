# Import necessary libraries
from datetime import datetime
from dotenv import load_dotenv
import os
from loguru import logger
import sys

# Import custom modules
from database.database_session import initialize_db
from fetch_data.fetch_equities import (
    fetch_and_store_profiles,
    fetch_and_store_financial_statements,
    fetch_and_store_hist_prices,
    fetch_and_store_market_cap_data,
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


if __name__ == "__main__":
    environment = "LOCAL"

    load_dotenv()
    configure_logger()

    run_id = datetime.now().strftime("%Y/%m/%d_%H:%M:%S")
    FMP_API_KEY = os.getenv("FMP_SECRET_KEY")

    db_session, engine = initialize_db(environment)

    common_args = {
        "engine": engine,
        "api_key": FMP_API_KEY,
        "SessionLocal": db_session,
        "run_id": run_id,
    }

    try:
        logger.bind(run_id=run_id).info("Starting data pipeline")

        # Fetch and store data
        fetch_and_store_profiles(**common_args)
        fetch_and_store_financial_statements(**common_args)
        fetch_and_store_hist_prices(**common_args)
        fetch_and_store_market_cap_data(**common_args)

    except Exception as e:
        logger.bind(run_id=run_id).exception("An error occurred: {}", e)
