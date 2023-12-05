import financedatabase as fd
import time

from investorkit.investorkit.get_data.base import (
    get_profile,
    get_financial_statements,
    get_historical_market_cap,
    get_historical_prices,
)

from utils.context_manager import session_scope
from utils.process_data import filter_bigint_range
from datetime import datetime
import pandas as pd
from loguru import logger
from database.db_connector import DBConnector

# Add the run_id to logger context
run_id = datetime.now().isoformat()
logger.bind(run_id=run_id)


# def store_to_db(df, table_name, db_connector, run_id):
#     try:
#         if db_connector.mongo_db is not None:
#             # MongoDB
#             db_connector.mongo_db.profiles.insert_many(df.to_dict(orient="records"))
#             # db_connector.write_to_mongo(table_name, df)
#         else:
#             # SQL Databases
#             with session_scope(db_connector.SessionLocal, run_id) as session:
#                 df.to_sql(
#                     table_name, con=db_connector.engine, if_exists="append", index=False
#                 )
#                 session.flush()

#         logger.bind(run_id=run_id).info(f"Stored data into {table_name}")
#     except Exception as e:
#         logger.bind(run_id=run_id).error(
#             f"Failed to store data into {table_name}. Error: {e}"
#         )


def store_to_db(df, table_name, db_connector, run_id):
    try:
        if df.empty:
            logger.bind(run_id=run_id).warning(f"No data to store in {table_name}.")
            return False

        if db_connector.mongo_db is not None:
            # MongoDB
            result = db_connector.mongo_db[table_name].insert_many(
                df.to_dict(orient="records")
            )
            if result.acknowledged:
                logger.bind(run_id=run_id).info(
                    f"Stored {len(result.inserted_ids)} records into {table_name}."
                )
                return True
            else:
                logger.bind(run_id=run_id).error(
                    f"Failed to store data into {table_name}."
                )
                return False
        else:
            # SQL Databases
            with session_scope(db_connector.SessionLocal, run_id) as session:
                df.to_sql(
                    table_name, con=db_connector.engine, if_exists="append", index=False
                )
                session.flush()
                logger.bind(run_id=run_id).info(f"Stored data into {table_name}.")
                return True
    except Exception as e:
        logger.bind(run_id=run_id).error(
            f"Failed to store data into {table_name}. Error: {e}"
        )
        return False


def fetch_equity_symbols(country="United States", market="NASDAQ Global Select"):
    try:
        equities = fd.Equities()
        selected_columns = [
            "name",
            "currency",
            "sector",
            "industry_group",
            "industry",
            "exchange",
            "market",
            "market_cap",
        ]
        us_equities = equities.select(country=country)
        df_equities = us_equities[us_equities["market"] == market][selected_columns]
        list_symbols = list(df_equities.index)
        logger.info(
            f"Fetched {len(list_symbols)} equity symbols for {country} - {market}"
        )
        return list_symbols
    except Exception as e:
        logger.exception(
            f"Failed to fetch equity symbols for {country} - {market}. Error: {e}"
        )


def get_new_symbols(list_symbols, db_connector):
    try:
        if db_connector.mongo_db is not None:
            existing_symbols_cursor = db_connector.mongo_db["profiles"].find(
                {"symbol": {"$in": list_symbols}}, {"symbol": 1}
            )
            existing_symbols_list = [doc["symbol"] for doc in existing_symbols_cursor]

            new_symbols = list(set(list_symbols) - set(existing_symbols_list))

        else:
            # SQL Databases
            existing_symbols_query = "SELECT symbol FROM profiles;"
            existing_symbols = pd.read_sql(
                existing_symbols_query, con=db_connector.engine
            )

            new_symbols = list(
                set(list_symbols) - set(existing_symbols["symbol"].tolist())
            )
        logger.info(f"Identified {len(new_symbols)} new symbols")
        return new_symbols
    except Exception as e:
        logger.exception(f"Failed to identify new symbols. Error: {e}")


def fetch_and_store_profiles(db_connector, api_key, run_id):
    list_symbols = [
        "AAPL",
        "MSFT",
        # "FB",
        # "TSLA",
        # "META",
        # "PYPL",
        # "PLTR",
        # "NIO",
        # "BRK-B",
    ]
    new_symbols = get_new_symbols(list_symbols, db_connector)

    if new_symbols:
        df_profiles = get_profile(new_symbols, api_key)

        if not df_profiles.empty:
            list_cols = [
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
            ]
            df_profiles_filtered = df_profiles[list_cols].copy()
            df_profiles_filtered["ipoDate"].replace("", None, inplace=True)
            df_profiles_filtered["cik"].replace("", None, inplace=True)

            df_profiles_filtered["ipoDate"] = pd.to_datetime(
                df_profiles_filtered["ipoDate"], errors="coerce"
            )

            store_to_db(df_profiles_filtered, "profiles", db_connector, run_id)
        else:
            logger.warning("No profiles found for the new symbols.")


def get_equity_symbols(db_connector, table_name="equities"):
    try:
        if db_connector.mongo_db is not None:
            # MongoDB operation

            # df_equities = db_connector.read_from_mongo(table_name)
            df_equities = db_connector.mongo_db.profiles.find({})
            list_symbols = []
            for df_equitie in df_equities:
                list_symbols.append(df_equitie["symbol"])

        else:
            # SQL operation
            query = f"SELECT DISTINCT symbol FROM {table_name};"
            df_equitie = pd.read_sql(query, con=db_connector.engine)
            list_symbols = set(df_equitie["symbol"].tolist())

        return list_symbols
    except Exception as e:
        logger.exception(f"Error fetching equity symbols: {e}")
        return []


# def fetch_and_store_financial_statements(db_connector, api_key, run_id):
#     try:
#         if db_connector.mongo_db is not None:
#             existing_symbols_df = []

#         else:
#             query = "SELECT DISTINCT symbol FROM cashflows;"
#             existing_symbols_df = pd.read_sql(query, con=db_connector.engine)

#             existing_symbols = set(existing_symbols_df["symbol"])

#         existing_symbols = []
#         list_symbols = get_equity_symbols(
#             db_connector,
#             "profiles",
#         )  # Assuming this function fetches all equity symbols

#         for symbol in list_symbols:
#             if symbol not in existing_symbols:
#                 df, invalid_tickers = get_financial_statements(
#                     tickers=symbol,
#                     statement="cashflow",
#                     api_key=api_key,
#                     start_date="2000-01-01",
#                 )
#                 filtered_df = filter_bigint_range(df, run_id)

#                 print(filtered_df)
#                 store_to_db(filtered_df, "cashflows", db_connector, run_id)

#         logger.info("Stored financial statements for all symbols.")
#     except Exception as e:
#         logger.exception(f"An error occurred while storing financial statements: {e}")


def fetch_and_store_financial_statements(db_connector, api_key, run_id):
    try:
        existing_symbols = get_equity_symbols(db_connector, "profiles")
        for symbol in existing_symbols:
            df, invalid_tickers = get_financial_statements(
                tickers=symbol,
                statement="cashflow",
                api_key=api_key,
                start_date="2000-01-01",
            )
            if not df.empty:
                # Cast integer fields to int64
                int64_fields = [
                    "cik",
                    "calendarYear",
                    "deferredIncomeTax",
                    "inventory",
                    "accountsPayables",
                    "otherNonCashItems",
                    "acquisitionsNet",
                    "otherInvestingActivites",
                    "netCashUsedForInvestingActivites",
                    "debtRepayment",
                    "commonStockIssued",
                    "effectOfForexChangesOnCash",
                    "netChangeInCash",
                    "commonStockRepurchased",
                    "dividendsPaid",
                    "otherFinancingActivites",
                    "netCashUsedProvidedByFinancingActivities",
                    "cashAtEndOfPeriod",
                    "cashAtBeginningOfPeriod",
                    "operatingCashFlow",
                    "capitalExpenditure",
                    "freeCashFlow",
                ]

                for field in int64_fields:
                    df[field] = df[field].astype("int64")

                # Convert date fields to datetime
                date_fields = ["fillingDate", "acceptedDate"]
                for field in date_fields:
                    df[field] = pd.to_datetime(df[field])

                filtered_df = filter_bigint_range(df, run_id)

                print(
                    filtered_df[
                        ["cik", "acceptedDate", "calendarYear", "freeCashFlow"]
                    ],
                )

                # Convert cik and calendarYear to 64-bit integers
                filtered_df["cik"] = filtered_df["cik"].astype("int64")
                filtered_df["calendarYear"] = filtered_df["calendarYear"].astype(
                    "int64"
                )

                store_to_db(
                    filtered_df[
                        ["cik", "acceptedDate", "calendarYear", "freeCashFlow"]
                    ],
                    "cashflows",
                    db_connector,
                    run_id,
                )
    except Exception as e:
        logger.exception(f"An error occurred while storing financial statements: {e}")


# def fetch_and_store_financial_statements(db_connector, api_key, run_id):
#     try:
#         existing_symbols = get_equity_symbols(db_connector, "profiles")
#         for symbol in existing_symbols:
#             df, invalid_tickers = get_financial_statements(
#                 tickers=symbol,
#                 statement="cashflow",
#                 api_key=api_key,
#                 start_date="2000-01-01",
#             )
#             if not df.empty:
#                 # Cast integer fields to int64
#                 int_fields = [
#                     "cik",
#                     "calendarYear",
#                     "deferredIncomeTax",
#                     "inventory",
#                     "accountsPayables",
#                     "otherNonCashItems",
#                     "acquisitionsNet",
#                     "otherInvestingActivites",
#                     "netCashUsedForInvestingActivites",
#                     "debtRepayment",
#                     "commonStockIssued",
#                     "effectOfForexChangesOnCash",
#                     "netIncome",
#                     "depreciationAndAmortization",
#                     "stockBasedCompensation",
#                     "changeInWorkingCapital",
#                     "accountsReceivables",
#                     "otherWorkingCapital",
#                     "netCashProvidedByOperatingActivities",
#                     "investmentsInPropertyPlantAndEquipment",
#                     "purchasesOfInvestments",
#                     "salesMaturitiesOfInvestments",
#                     "commonStockRepurchased",
#                     "dividendsPaid",
#                     "otherFinancingActivites",
#                     "netCashUsedProvidedByFinancingActivities",
#                     "netChangeInCash",
#                     "cashAtEndOfPeriod",
#                     "cashAtBeginningOfPeriod",
#                     "operatingCashFlow",
#                     "capitalExpenditure",
#                     "freeCashFlow",
#                 ]
#                 for field in int_fields:
#                     df[field] = df[field].astype("int64")

#                 # Convert date fields to datetime
#                 date_fields = ["fillingDate", "acceptedDate"]
#                 for field in date_fields:
#                     df[field] = pd.to_datetime(df[field])

#                 filtered_df = filter_bigint_range(df, run_id)

#                 # Convert necessary fields to 'int64'
#                 int64_fields = [
#                     "cik",
#                     "calendarYear",
#                     "deferredIncomeTax",
#                     "inventory",
#                     "accountsPayables",
#                     "otherNonCashItems",
#                     "acquisitionsNet",
#                     "otherInvestingActivites",
#                     "netCashUsedForInvestingActivites",
#                     "debtRepayment",
#                     "commonStockIssued",
#                     "effectOfForexChangesOnCash",
#                     # Add other fields that need to be converted to 'int64'
#                 ]
#                 for field in int64_fields:
#                     filtered_df[field] = filtered_df[field].astype("int64")

#                 # Convert date fields to proper datetime objects for MongoDB
#                 date_fields = ["fillingDate", "acceptedDate"]
#                 for field in date_fields:
#                     filtered_df[field] = pd.to_datetime(filtered_df[field])

#                 store_to_db(filtered_df, "cashflows", db_connector, run_id)
#     except Exception as e:
#         logger.exception(f"An error occurred while storing financial statements: {e}")


def fetch_and_store_hist_prices(db_connector, api_key, run_id, chunk_size=100):
    try:
        list_symbols = get_equity_symbols(db_connector, "hist_prices")
        for i in range(0, len(list_symbols), chunk_size):
            chunk_symbols = list_symbols[i : i + chunk_size]
            for symbol in chunk_symbols:
                df = get_historical_prices(symbol, api_key)
                filtered_df = filter_bigint_range(df, run_id)
                store_to_db(filtered_df, "hist_prices", db_connector, run_id)

        logger.info("Stored historical prices for all symbols.")
    except Exception as e:
        logger.exception(
            f"An error occurred while fetching and storing historical prices: {e}"
        )


def fetch_and_store_market_cap_data(db_connector, api_key, run_id, chunk_size=100):
    try:
        list_symbols = get_equity_symbols(db_connector, "market_cap_data")
        for i in range(0, len(list_symbols), chunk_size):
            chunk_symbols = list_symbols[i : i + chunk_size]
            for symbol in chunk_symbols:
                df = get_historical_market_cap(symbol, api_key)
                filtered_df = filter_bigint_range(df, run_id)
                store_to_db(filtered_df, "hist_marketcap", db_connector, run_id)

        logger.info("Stored market cap data for all symbols.")
    except Exception as e:
        logger.exception(
            f"An error occurred while fetching and storing market cap data: {e}"
        )
