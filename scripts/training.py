from typing import Tuple, List
import pandas as pd
import logging
from pathlib import Path

def create_ml_training_df(
    df: pd.DataFrame,
    config: dict,
    logger: logging.Logger
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Filters DataFrame into ML-ready subset based on config['ml_columns'] and 'total unique count'.

    Returns:
        - MLdf: rows where count > 0
        - MLdf_dropped: rows where count == 0
    """
    # Validate required column exists
    required_col = "total unique count"
    if required_col not in df.columns:
        logger.error(f" Column '{required_col}' not found in enriched DataFrame.")
        return pd.DataFrame(), pd.DataFrame()

    columns_to_extract: List[str] = config.get("ml_columns", [])

    if not columns_to_extract:
        logger.warning(" No columns defined in 'ml_columns' for training output.")
        return pd.DataFrame(), pd.DataFrame()

    try:
        df_retained = df[df["total unique count"] > 0]
        df_dropped = df[df["total unique count"] == 0]

        MLdf = df_retained[columns_to_extract]
        MLdf_dropped = df_dropped[columns_to_extract]

        finalize_summary = {
            "ml_columns_used": columns_to_extract,  # Should be list[str]
            "total_input_rows": int(df.shape[0]),
            "total_retained_rows": int(df_retained.shape[0]),
            "total_dropped_rows": int(df_dropped.shape[0]),
            "percent_retained": round((df_retained.shape[0] / df.shape[0]) * 100, 2) if df.shape[0] > 0 else None,
            "percent_dropped": round((df_dropped.shape[0] / df.shape[0]) * 100, 2) if df.shape[0] > 0 else None,
            "summary_type": "ML Training Filter",
            "retained_index_range": [int(df_retained.index.min()), int(df_retained.index.max())] if not df_retained.empty else None,
            "dropped_index_range": [int(df_dropped.index.min()), int(df_dropped.index.max())] if not df_dropped.empty else None
        }


        logger.info(f" Retained {MLdf.shape[0]} training rows, dropped {MLdf_dropped.shape[0]}")
        logger.info(f" Columns used for ML: {columns_to_extract}")

        return MLdf, MLdf_dropped, finalize_summary

    except KeyError as e:
        logger.error(" Column(s) missing from DataFrame during ML filtering", exc_info=True)
        raise
    except Exception as e:
        logger.error(" Unexpected error in ML training filter", exc_info=True)
        raise