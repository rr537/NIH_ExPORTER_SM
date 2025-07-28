from logging import config
from typing import Dict, List, Tuple
import pandas as pd
import logging

def validate_csv_headers_from_df(
    config: dict,
    df: pd.DataFrame,
    logger: logging.Logger,
    label: str = "Aggregated Output"
) -> Dict[str, List[str]]:
    """
    Validates whether expected drop columns are present in a single aggregated dataframe.

    Returns:
        Dict mapping folder names to missing columns (only if mismatches exist).
    """
    logger.info(f" Starting header validation for '{label}'...")
    total_missing_headers = {}

    drop_map = config.get("drop_col_header_map", {})
    if not drop_map:
        logger.warning(" No drop_col_header_map found in config — skipping header validation.")
        return {}

    actual_headers = set(df.columns)

    for folder, expected_cols in drop_map.items():
        missing = set(expected_cols) - actual_headers
        matched = set(expected_cols) & actual_headers

        if matched:
            logger.info(f" '{folder}' has {len(matched)} column(s) eligible for dropping: {sorted(matched)}")
        if missing:
            logger.warning(f" '{folder}' is missing expected columns: {sorted(missing)}")
            total_missing_headers[folder] = sorted(missing)
        else:
            logger.info(f" '{folder}' passed header validation.")

    if not total_missing_headers:
        logger.info(" All drop folders passed header validation.")
        
    return total_missing_headers

def drop_specified_columns_from_df(
    config: dict,
    df: pd.DataFrame,
    logger: logging.Logger,
    label: str = "Aggregated Output"
) -> pd.DataFrame:
    """
    Drops specified columns from a flat DataFrame using 'drop_col_header_map' config.

    Returns:
        Modified DataFrame with columns dropped.
    """
    logger.info(f" Dropping columns from '{label}'...")
    drop_map = config.get("drop_col_header_map", {})

    # Merge all drop rules from different folders
    all_cols_to_drop = set()
    for folder, cols in drop_map.items():
        all_cols_to_drop.update(cols)

    existing = [col for col in all_cols_to_drop if col in df.columns]
    missing = [col for col in all_cols_to_drop if col not in df.columns]

    if existing:
        logger.info(f" Dropping {len(existing)} columns from '{label}': {existing}")
        df.drop(columns=existing, inplace=True)

    if missing:
        logger.debug(f" Columns not found in '{label}' for dropping: {missing}")

    return df

from typing import Tuple, Dict
import pandas as pd
import logging

def rename_dataframe_columns(
    config: dict,
    raw_dict: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> Tuple[Dict[str, Dict[str, pd.DataFrame]], Dict[str, list]]:
    """
    Applies renaming rules to all DataFrames using config['rename_columns_map'],
    logging detailed column changes and returning the renamed structure.
    
    Returns:
        - renamed_dict: DataFrames with updated column names
        - rename_summary: Dict summarizing per-file column changes
    """
    logger.info("Renaming columns using configured rules...")
    rename_mapping = config.get("rename_columns_map", {})
    rename_summary = {}
    renamed_dict = {}

    if not rename_mapping:
        logger.warning(" No renaming rules found in config['rename_columns_map'].")
        return raw_dict, rename_summary

    logger.info(f" Loaded {len(rename_mapping)} column renaming rules.")

    for folder, files in raw_dict.items():
        renamed_dict[folder] = {}
        for file_name, df in files.items():
            original = df.columns.tolist()
            df_renamed = df.rename(columns=rename_mapping)
            updated = df_renamed.columns.tolist()

            changed = [
                f"{old} -> {rename_mapping[old]}"
                for old in rename_mapping
                if old in original and rename_mapping[old] in updated
            ]

            if changed:
                logger.info(f"[{file_name}] Renamed {len(changed)} column(s): {', '.join(changed)}")
                rename_summary[file_name] = changed

            renamed_dict[folder][file_name] = df_renamed

    return renamed_dict, rename_summary