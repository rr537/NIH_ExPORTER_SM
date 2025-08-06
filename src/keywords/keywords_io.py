import pandas as pd
from pathlib import Path
from typing import Optional, Union, Dict
import logging
import json

def load_metrics_dataframe(
    keywords_path: Path,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Loads the keywords CSV into a DataFrame.

    Parameters:
        keywords_path: Path to 'keywords.csv'
        logger: optional logger instance

    Returns:
        Loaded DataFrame
    """
    if logger:
        logger.info("Loading keywords DataFrame...")

    keywords_df = pd.read_csv(keywords_path, low_memory=False)

    if logger:
        logger.info(f"Loaded keywords DataFrame: {keywords_df.shape[0]:,} rows × {keywords_df.shape[1]:,} columns")

    return keywords_df

def export_keywords_csv(
    keywords_df: pd.DataFrame,
    output_dir: Union[str, Path],
    logger: Optional[logging.Logger]
) -> Path:
    """
    Saves the keyword-enriched DataFrame to keywords.csv in the output directory.

    Args:
        keywords_df: DataFrame with keyword enrichment results
        output_dir: Path to save keywords.csv
        logger: Active logger instance

    Returns:
        Path to saved CSV file
    """
    keywords_path = output_dir / "keywords.csv"
    keywords_df.to_csv(keywords_path, index=False)
    logger.info(f"Keyword-enriched DataFrame saved to: {keywords_path}")

    return keywords_path