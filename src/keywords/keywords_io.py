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
    logger: logging.Logger
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
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    keywords_path = output_dir / "keywords.csv"
    keywords_df.to_csv(keywords_path, index=False)
    logger.info(f"Keyword-enriched DataFrame saved to: {keywords_path}")

    return keywords_path

def export_summary_json(
    summary: Dict,
    output_path: Union[str, Path],
    summary_path: Optional[Union[str, Path]] = None,
    logger: Optional[logging.Logger] = None
) -> Path:
    """
    Exports the keyword enrichment summary to a JSON file.

    Args:
        summary: Dictionary containing enrichment metrics
        output_path: Base output directory
        summary_path: Optional custom summary path
        logger: Optional logger for status messages

    Returns:
        Path to the saved JSON summary
    """
    output_path = Path(output_path).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    final_path = Path(summary_path).resolve() if summary_path else output_path / "keywords_summary.json"

    with open(final_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    if logger:
        logger.info(f"Keywords summary exported to: {final_path}")

    return final_path