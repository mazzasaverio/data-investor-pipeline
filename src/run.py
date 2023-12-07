import os

from dotenv import load_dotenv
from loguru import logger
from utils.utils_logger import configure_logger

from data_ingestion.db_connector import DBConnector
from data_ingestion.api_financial_modeling_prep.fetch_financial_statement import (
    fetch_and_store_data,
)


def main():
    load_dotenv()

    FMP_SECRET_KEY = os.getenv("FMP_SECRET_KEY")

    config_params = {
        "period": "quarter",
        "apikey": FMP_SECRET_KEY,
        "limit": 20,
        "batch_size": 10,
        "base_url": "https://financialmodelingprep.com/api/v3",
        "store_data": True,
    }
    # Data tables and symbols
    dict_tables = {
        "income-statement": "income_statement",
        "balance-sheet-statement": "balance_sheet_statement",
        "cash-flow-statement": "cash_flow_statement",
    }
    symbols = ["AAPL", "MSFT", "GOOG", "TSLA"]

    # Initialize DBConnector
    db = DBConnector()
    db.initialize_db("MONGO")

    df = fetch_and_store_data(symbols, dict_tables, config_params, db)


if __name__ == "__main__":
    main()
