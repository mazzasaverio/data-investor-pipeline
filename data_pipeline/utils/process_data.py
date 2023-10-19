import numpy as np
import pandas as pd
from common.constants import MIN_BIGINT, MAX_BIGINT
from loguru import logger


def filter_bigint_range(df: pd.DataFrame, run_id) -> pd.DataFrame:
    try:
        min_bigint, max_bigint = MIN_BIGINT, MAX_BIGINT
        numeric_cols = df.select_dtypes(include=[np.number]).columns

        for col in numeric_cols:
            df = df.query(f"{min_bigint} <= {col} <= {max_bigint}")

        logger.bind(run_id=run_id).info(
            f"Filtered DataFrame based on the bigint range."
        )
        return df
    except Exception as e:
        logger.bind(run_id=run_id).exception(
            f"An error occurred while filtering DataFrame: {e}"
        )
        return pd.DataFrame()
