from datetime import datetime
from loguru import logger
from fetch_data.fetch_equities import (
    fetch_and_store_profiles,
    fetch_and_store_financial_statements,
    fetch_and_store_hist_prices,  # Add this line
    fetch_and_store_market_cap_data,  # Add this line
)
from database.database_session import SessionLocal, engine
from common.config import FMP_API_KEY


logger.add("./logs/data_pipeline.log", rotation="1 day", level="INFO", serialize=True)


if __name__ == "__main__":
    run_id = datetime.now().isoformat()
    common_args = {
        "engine": engine,
        "api_key": FMP_API_KEY,
        "SessionLocal": SessionLocal,
        "run_id": run_id,
    }

    try:
        logger.bind(run_id=run_id).info("Starting data pipeline")

        fetch_and_store_profiles(**common_args)
        fetch_and_store_financial_statements(**common_args)
        fetch_and_store_hist_prices(**common_args)  # Add this line
        fetch_and_store_market_cap_data(**common_args)  # Add this line

    except Exception as e:
        logger.bind(run_id=run_id).exception("An error occurred: {}", e)
