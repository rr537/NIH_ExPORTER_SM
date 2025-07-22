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
    columns_to_extract: List[str] = config.get("ml_columns", [])

    if not columns_to_extract:
        logger.warning("⚠️ No columns defined in 'ml_columns' for training output.")
        return pd.DataFrame(), pd.DataFrame()

    try:
        df_retained = df[df["total unique count"] > 0]
        df_dropped = df[df["total unique count"] == 0]

        MLdf = df_retained[columns_to_extract]
        MLdf_dropped = df_dropped[columns_to_extract]

        logger.info(f"🧠 Retained {MLdf.shape[0]} training rows, dropped {MLdf_dropped.shape[0]}")
        logger.info(f"📦 Columns used for ML: {columns_to_extract}")

        return MLdf, MLdf_dropped

    except KeyError as e:
        logger.error("❌ Column(s) missing from DataFrame during ML filtering", exc_info=True)
        raise
    except Exception as e:
        logger.error("❌ Unexpected error in ML training filter", exc_info=True)
        raise

def export_training_dataframe(
    df: pd.DataFrame,
    config: dict,
    logger: logging.Logger,
    filename: str
) -> None:
    """
    Exports ML training DataFrame to output directory defined in config.
    """
    output_dir = Path(config.get("output_dir", "results")).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"📁 Export path: {output_dir}")

    out_path = output_dir / filename
    df.to_csv(out_path, index=False)

    logger.info(f"✅ ML training DataFrame saved to: {out_path}")