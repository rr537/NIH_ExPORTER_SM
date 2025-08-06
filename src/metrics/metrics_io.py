import json
import pandas as pd
from pathlib import Path
from typing import Optional, Dict, Any, Union 
import logging 

def load_pickle_dataframes(
    pickle_map: Dict[str, Union[str, Path]],
    logger: Optional[logging.Logger] = None
) -> Dict[str, pd.DataFrame]:
    """
    Loads DataFrames from a dict of name → pickle file path.

    Parameters:
        pickle_map: dict mapping keys (e.g. folder names) to pickle file paths.
        logger: optional logger for diagnostics.

    Returns:
        Dict of key → loaded DataFrame
    """
    if logger:
        logger.info("Loading preprocessed DataFrames from pickle files...")

    loaded_dict = {}

    for name, path in pickle_map.items():
        df = pd.read_pickle(path)
        loaded_dict[name] = df

        if logger:
            logger.info(f"  └─ {name}: {df.shape[0]:,} rows × {df.shape[1]:,} columns")

    if logger:
        logger.info(f"Loaded {len(loaded_dict)} pickle file(s).")

    return loaded_dict

def export_metrics_csv(
    metrics_df: pd.DataFrame,
    output_dir: Path,
    logger: Optional[logging.Logger] = None
) -> Path:
    """
    Exports the metrics DataFrame to CSV in the specified output directory.
    """
    metrics_path = output_dir / "metrics.csv"
    metrics_df.to_csv(metrics_path, index=False)

    if logger:
        logger.info(f"Metrics DataFrame saved to: {metrics_path}")

    return metrics_path
