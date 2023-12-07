from utils.utils_data import get_jsonparsed_data
from tqdm import tqdm
import polars as pl


def fetch_data_for_symbol(symbol, data_flow, params):
    period = params.get("period")
    api_key = params.get("apikey")
    limit = params.get("limit")

    url = f"{params.get('base_url')}/{data_flow}/{symbol}?period={period}&apikey={api_key}&limit={limit}"

    data = get_jsonparsed_data(url)

    return data


def store_unique_data(df, collection_name, db):
    dedup_fields = [
        "symbol",
        "date",
        "cik",
        "fillingDate",
        "acceptedDate",
        "calendarYear",
        "period",
    ]
    existing_df = db.read_from_mongo(collection_name, {})
    if existing_df.height > 0:
        # Deduplicate against existing data
        unique_data = df.join(existing_df, on=dedup_fields, how="anti")
    else:
        unique_data = df

    if unique_data.height > 0:
        db.write_to_mongo(collection_name, unique_data)


def fetch_and_store_data(symbols, dict_tables, config_params, db):
    # Process symbols in batches of 'batch_size'
    batch_size = config_params.get("batch_size")
    for data_flow in dict_tables.keys():
        for i in range(0, len(symbols), batch_size):
            batch_symbols = symbols[i : i + batch_size]
            batch_data = pl.DataFrame()

            for symbol in tqdm(batch_symbols):
                symbol_data = pl.DataFrame()

                table = dict_tables[data_flow]
                data = fetch_data_for_symbol(symbol, data_flow, config_params)

                if data:
                    df = pl.DataFrame(data)

                    symbol_data = pl.concat([symbol_data, df])

                if symbol_data.height > 0:
                    batch_data = pl.concat([batch_data, symbol_data])

            print(f"{batch_data.height} rows fetched for {data_flow}")
            if config_params.get("store_data"):
                store_unique_data(batch_data, table, db)

    return batch_data
