from typing import Tuple, List, Dict
import pandas as pd
import logging


def create_ml_training_df(
    df: pd.DataFrame,
    config: Dict,
    logger: logging.Logger
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Filters DataFrame into ML-ready subset using 'ml_columns' and 'total unique count'.

    Args:
        df (pd.DataFrame): Input DataFrame after keyword enrichment.
        config (dict): Config dictionary with key 'ml_columns'.
        logger (logging.Logger): Logger instance for capturing messages.

    Returns:
        Tuple containing:
            - MLdf (pd.DataFrame): Subset where count > 0
            - MLdf_dropped (pd.DataFrame): Subset where count == 0
            - finalize_summary (dict): Metadata summary of filtering step
    """
    # Set column that will be used to filter rows
    required_col = "total unique count"

    # Check if required input dataframe is empty  
    if df.empty:
        logger.warning("Input DataFrame is empty.")
        return pd.DataFrame(), pd.DataFrame(), {}

    # Check if column for filtering is present in the input dataframe
    if required_col not in df.columns:
        logger.error(f"Column '{required_col}' not found in DataFrame.")
        return pd.DataFrame(), pd.DataFrame(), {}

    # Load 'ml_columns' from config
    columns_to_extract: List[str] = config.get("ml_columns", [])
    if not columns_to_extract:
        logger.warning("No columns defined in 'ml_columns' for training output.")
        return pd.DataFrame(), pd.DataFrame(), {}

    # Find any missing columns from 'ml_columns' that are not present in the input dataframe 
    missing_cols = [col for col in columns_to_extract if col not in df.columns]
    if missing_cols:
        logger.error(f"Missing ML columns: {missing_cols}")
        return pd.DataFrame(), pd.DataFrame(), {}

    # Filter and split the dataframe based on the required column value
    try:
        df_retained = df[df[required_col] > 0]
        df_dropped = df[df[required_col] == 0]

        MLdf = df_retained[columns_to_extract]
        MLdf_dropped = df_dropped[columns_to_extract]

    # Create a summary dictionary with metadata about the filtering process
        finalize_summary = {
            "ml_columns_used": columns_to_extract,
            "total_input_rows": int(df.shape[0]),
            "total_retained_rows": int(df_retained.shape[0]),
            "total_dropped_rows": int(df_dropped.shape[0]),
            "percent_retained": round((df_retained.shape[0] / df.shape[0]) * 100, 2) if df.shape[0] > 0 else None,
            "percent_dropped": round((df_dropped.shape[0] / df.shape[0]) * 100, 2) if df.shape[0] > 0 else None,
            "summary_type": "ML Training Filter",
            "retained_index_range": [int(df_retained.index.min()), int(df_retained.index.max())] if not df_retained.empty else None,
            "dropped_index_range": [int(df_dropped.index.min()), int(df_dropped.index.max())] if not df_dropped.empty else None
        }

        logger.info(f"Retained {MLdf.shape[0]} training rows, dropped {MLdf_dropped.shape[0]}")
        logger.info(f"Columns used for ML: {columns_to_extract}")

        return MLdf, MLdf_dropped, finalize_summary

    except KeyError as e:
        logger.error("Column(s) missing during ML filtering", exc_info=True)
        raise
    except Exception as e:
        logger.error("Unexpected error in ML training filter", exc_info=True)
        raise