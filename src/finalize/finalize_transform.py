from typing import Tuple, List, Dict, Optional 
import pandas as pd
import logging

def filter_df(
    df: pd.DataFrame,
    config: Dict,
    logger: Optional[logging.Logger] = None,
    cutoff_value: Optional[int] = None
) -> Tuple[pd.DataFrame, pd.DataFrame, Dict]:
    """
    Filters DataFrame into ML-ready subset using 'ml_columns' and 'total unique count'.

    Returns:
        Tuple:
            - MLdf: DataFrame where count ≥ cutoff
            - MLdf_dropped: DataFrame where count < cutoff
            - finalize_summary: Summary dictionary of filtering process
    """
    required_col = "total unique count"

    if df.empty:
        if logger:
            logger.warning("Input DataFrame is empty.")
        return pd.DataFrame(), pd.DataFrame(), {}

    if required_col not in df.columns:
        if logger:
            logger.error(f"Column '{required_col}' not found in DataFrame.")
        return pd.DataFrame(), pd.DataFrame(), {}

    columns_to_extract: List[str] = config.get("ml_columns", [])
    if not columns_to_extract:
        if logger:
            logger.warning("No columns defined in 'ml_columns' for training output.")
        return pd.DataFrame(), pd.DataFrame(), {}

    missing_cols = [col for col in columns_to_extract if col not in df.columns]
    if missing_cols:
        if logger:
            logger.error(f"Missing ML columns: {missing_cols}")
        return pd.DataFrame(), pd.DataFrame(), {}

    try:
        cutoff_value = int(cutoff_value) if cutoff_value is not None else int(config.get("cutoff_value", 0))

        df_retained = df[df[required_col] >= cutoff_value]
        df_dropped = df[df[required_col] < cutoff_value]

        MLdf = df_retained[columns_to_extract]
        MLdf_dropped = df_dropped[columns_to_extract]

        finalize_summary = {
            "ml_columns_used": columns_to_extract,
            "cutoff_value": cutoff_value,
            "total_input_rows": int(df.shape[0]),
            "total_retained_rows": int(df_retained.shape[0]),
            "total_dropped_rows": int(df_dropped.shape[0]),
            "percent_retained": round((df_retained.shape[0] / df.shape[0]) * 100, 2) if df.shape[0] > 0 else None,
            "percent_dropped": round((df_dropped.shape[0] / df.shape[0]) * 100, 2) if df.shape[0] > 0 else None,
            "retained_index_range": [int(df_retained.index.min()), int(df_retained.index.max())] if not df_retained.empty else None,
            "dropped_index_range": [int(df_dropped.index.min()), int(df_dropped.index.max())] if not df_dropped.empty else None
        }

        if logger:
            logger.info(f"Retained {MLdf.shape[0]} training rows, dropped {MLdf_dropped.shape[0]}")
            logger.info(f"Columns used for ML: {columns_to_extract}")

        return MLdf, MLdf_dropped, finalize_summary

    except KeyError:
        if logger:
            logger.error("Column(s) missing during ML filtering", exc_info=True)
        raise
    except Exception:
        if logger:
            logger.error("Unexpected error in ML training filter", exc_info=True)
        raise
