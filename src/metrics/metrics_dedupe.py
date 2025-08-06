import pandas as pd
import logging
from typing import Tuple, Optional

def remove_true_duplicates_from_df(
    df: pd.DataFrame,
    logger: Optional[logging.Logger] = None
) -> Tuple[pd.DataFrame, dict]:
    """
    Detects and removes fully duplicated rows from a DataFrame.

    Returns:
        Tuple:
        - Deduplicated DataFrame
        - Summary dict of duplication metrics:
            {
                "Aggregate_output": {
                    "unique_duplicate_rows": int,
                    "total_duplicates": int,
                    "extra_duplicates": int
                }
            }
    """
    
    if logger:
        logger.info(f"Checking for true duplicates in 'Aggregate_output'...")

    dedupe_df = df.copy()

    # Convert list-like columns to hashable tuples
    for col in dedupe_df.columns:
        if dedupe_df[col].apply(lambda x: isinstance(x, list)).any():
            dedupe_df[col] = dedupe_df[col].map(lambda x: tuple(x) if isinstance(x, list) else x)

    # Compute all duplicate metrics
    duplicates_all = dedupe_df.duplicated(keep=False)
    duplicates_extra = dedupe_df.duplicated(keep="first") 
    distinct_duplicate_rows = dedupe_df[duplicates_all].drop_duplicates()

    total_duplicates = duplicates_all.sum()
    extra_duplicates = duplicates_extra.sum()
    unique_duplicate_rows = distinct_duplicate_rows.shape[0]

    # Summary metrics
    dedupe_summary_dict = {
        "Aggregate_output": {
            "unique_duplicate_rows": int(unique_duplicate_rows),
            "total_duplicates": int(total_duplicates),
            "extra_duplicates": int(extra_duplicates)
        }
    }

    if logger:
        logger.info(
            f"{unique_duplicate_rows:,} unique duplicates, "
            f"{total_duplicates:,} total, {extra_duplicates:,} extra."
        )

    # Drop all duplicates
    if total_duplicates > 0:
        if logger:
            logger.info(f"Found {total_duplicates:,} duplicate rows in 'Aggregate_output'. Showing sample...")
            logger.info(dedupe_df[duplicates_all].head())
        dedupe_df = dedupe_df.drop_duplicates()
        if logger:
            logger.info(f"Duplicates removed — new shape: {dedupe_df.shape[0]:,} rows × {dedupe_df.shape[1]:,} columns")
    else:
        if logger:
            logger.info(f"No duplicate rows found in 'Aggregate_output'.")

    return dedupe_df, dedupe_summary_dict
