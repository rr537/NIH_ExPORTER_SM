from logging import config
from typing import Dict, List, Tuple
import pandas as pd
import logging

def validate_csv_headers(config: dict, dataframes: Dict[str, Dict[str, pd.DataFrame]], logger: logging.Logger) -> Dict[str, List[str]]:
    """
    Validates whether expected drop columns are present in loaded dataframes.

    Returns:
        Dict mapping folder names to missing columns (only if mismatches exist).
    """
    total_missing_headers = {}
    missing_folder_configs = []

    for folder, file_dfs in dataframes.items():
        expected_drop_cols = config.get("drop_col_header_map", {}).get(folder)

        if expected_drop_cols is None:
            missing_folder_configs.append(folder)
            continue

        actual_headers = set()
        for df in file_dfs.values():
            actual_headers.update(df.columns)

        missing = set(expected_drop_cols) - actual_headers
        matched = set(expected_drop_cols) & actual_headers

        if matched:
            logger.info(f"'{folder}' has {len(matched)} column(s) eligible for dropping: {sorted(matched)}")

        if missing:
            logger.warning(f"'{folder}' is missing expected columns: {sorted(missing)}")
            total_missing_headers[folder] = sorted(missing)
        else:
            logger.info(f"'{folder}' passed header validation.")

    if missing_folder_configs:
        logger.warning(f"No config found for: {', '.join(missing_folder_configs)}")

    if not total_missing_headers and not missing_folder_configs:
        logger.info(" All folders passed header validation.")

    return total_missing_headers

def drop_specified_columns(
    config: dict,
    dataframes: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> None:
    """
    Drops columns from each DataFrame based on folder-specific rules.

    Args:
        config: Parsed YAML configuration.
        dataframes: Dict of folder -> file -> DataFrame.
        logger: Logger instance for messages.
    """
    columns_to_drop = config.get("drop_col_header_map", {})

    if not columns_to_drop:
        logger.warning(" No column drop configuration found in 'drop_col_header_map'.")
        return

    for folder, file_dfs in dataframes.items():
        drop_cols = columns_to_drop.get(folder)

        if drop_cols is None:
            logger.info(f" No drop rules configured for folder '{folder}' — skipping.")
            continue

        logger.info(f" Drop rule for '{folder}': {len(drop_cols)} columns -> {drop_cols}")

        for file_name, df in file_dfs.items():
            existing = [col for col in drop_cols if col in df.columns]
            missing = [col for col in drop_cols if col not in df.columns]

            if existing:
                df.drop(columns=existing, inplace=True)
                logger.info(f" Dropped {len(existing)} columns from '{file_name}' in '{folder}': {existing}")

            if missing:
                logger.debug(f" Columns not found in '{file_name}' for dropping: {missing}")

def rename_dataframe_columns(
    config: dict,
    dataframes: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> None:
    """
    Applies renaming rules to all DataFrames using config['rename_columns_map'].
    """
    rename_mapping = config.get("rename_columns_map", {})

    if not rename_mapping:
        logger.warning(" No renaming rules found in config['rename_columns_map'].")
        return

    logger.info(f" Loaded {len(rename_mapping)} renaming rules from config.")

    for folder, files in dataframes.items():
        for file_name, df in files.items():
            original = df.columns.tolist()
            df.rename(columns=rename_mapping, inplace=True)
            updated = df.columns.tolist()

            changed = [
                f"{old} -> {rename_mapping[old]}"
                for old in rename_mapping
                if old in original and rename_mapping[old] in updated
            ]

            if changed:
                logger.info(f"[{file_name}] Renamed {len(changed)} column(s): {', '.join(changed)}")