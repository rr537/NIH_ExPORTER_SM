import pandas as pd
from pathlib import Path
from typing import Optional, Union, Dict, Any
import logging
import json

def load_keywords_dataframe(
    keywords_path: str,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Load the keywords-enriched DataFrame from a CSV file.

    Args:
        keywords_path: Path to 'keywords.csv'
        logger: Optional logger for diagnostics

    Returns:
        Loaded DataFrame
    """
    if logger:
        logger.info("Loading keywords-enriched DataFrame...")

    path = Path(keywords_path).resolve()
    if not path.exists():
        if logger:
            logger.error(f"Keywords file not found: {path}")
        raise FileNotFoundError(f"Keywords file does not exist: {path}")

    keywords_df = pd.read_csv(path, low_memory=False)

    if logger:
        logger.info(f"Loaded keywords DataFrame: {keywords_df.shape[0]:,} rows × {keywords_df.shape[1]:,} columns")

    return keywords_df

def export_finalized_csv(
    finalized_df: pd.DataFrame,
    dropped_df: Optional[pd.DataFrame],
    output_dir: Union[str, Path],
    config: Dict,
    logger: Optional[logging.Logger] = None
) -> Dict[str, Path]:
    """
    Exports the finalized DataFrame to 'finalized.csv' and optionally the dropped rows.

    Args:
        finalized_df: Final DataFrame to export.
        output_dir: Directory to save the CSV files.
        logger: Optional logger for status messages.
        dropped_df: Optional DataFrame of dropped rows.
        drop_rows: Whether to export dropped rows.

    Returns:
        Dictionary containing paths to saved CSV(s).
    """

    paths = {}

    # Save finalized DataFrame
    finalized_path = output_dir / "finalized.csv"
    finalized_df.to_csv(finalized_path, index=False)
    paths["finalized"] = finalized_path

    if logger:
        logger.info(f"Finalized DataFrame saved to: {finalized_path}")

    # Fetch config-based flag
    export_dropped = config.get("export_drop_output", False)

    # Save dropped rows if applicable
    if export_dropped and dropped_df is not None:
        dropped_path = output_dir / "dropped_rows.csv"
        dropped_df.to_csv(dropped_path, index=False)
        paths["dropped"] = dropped_path

        if logger:
            logger.info(f"Dropped rows saved to: {dropped_path}")

    return paths