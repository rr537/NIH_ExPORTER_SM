import pandas as pd
import logging
from typing import Dict, Tuple, List, Any

# Column Renaming
def rename_columns(
    config: dict,
    raw_dict: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> Tuple[Dict[str, Dict[str, pd.DataFrame]], Dict[str, List[str]]]:
    """
    Renames columns in all DataFrames using config['rename_columns_map'].
    Returns updated DataFrames and a summary of changes per file.
    """
    rename_map = config.get("rename_columns_map", {})
    if not rename_map:
        logger.warning("No renaming rules found in config['rename_columns_map'].")
        return raw_dict, {}

    logger.info(f"Applying {len(rename_map)} column renaming rules...")
    renamed_dict = {}
    rename_summary = {}

    for folder, files in raw_dict.items():
        renamed_dict[folder] = {}
        for file_name, df in files.items():
            original = df.columns.tolist()
            renamed_df = df.rename(columns=rename_map)
            updated = renamed_df.columns.tolist()

            changes = [
                f"{col} -> {rename_map[col]}"
                for col in rename_map
                if col in original and rename_map[col] in updated
            ]

            if changes:
                logger.info(f"[{file_name}] Renamed columns: {', '.join(changes)}")
                rename_summary[file_name] = changes

            renamed_dict[folder][file_name] = renamed_df

    return renamed_dict, rename_summary

# Folder-wise DataFrame Appending
def append_dataframes_by_folder(
    config: dict,
    dataframes: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, Any]]]:
    """
    Concatenates DataFrames within each folder. Handles schema mismatch and logs metadata.
    """
    force_append = config.get("force_append", False)
    appended_dict = {}
    summary_dict = {}

    for folder, dfs in dataframes.items():
        summary = _initialize_folder_summary(folder, len(dfs))

        try:
            all_columns = {col for df in dfs.values() for col in df.columns}
            common_columns = set.intersection(*(set(df.columns) for df in dfs.values()))
            unexpected = sorted(all_columns - common_columns)

            summary["unexpected_columns"] = unexpected
            summary["unexpected_columns_added"] = len(unexpected)

            if unexpected and not force_append:
                logger.warning(f"Column mismatch in '{folder}': {unexpected}")
                summary["skipped"] = True
                logger.info(f"Skipping append for '{folder}' due to mismatch.")
                summary_dict[folder] = summary
                continue

            appended_df = pd.concat(list(dfs.values()), ignore_index=True)
            appended_dict[folder] = appended_df

            summary["total_rows"] = appended_df.shape[0]
            summary["total_columns"] = appended_df.shape[1]
            logger.info(
                f"Appended {summary['num_files']} files in '{folder}': "
                f"{summary['total_rows']:,} rows, {summary['total_columns']:,} columns."
            )

        except Exception as e:
            summary["error"] = str(e)
            logger.error(f"Error in folder '{folder}': {str(e)}", exc_info=True)

        summary_dict[folder] = summary

    return appended_dict, summary_dict

# Helper: Folder Summary
def _initialize_folder_summary(folder: str, num_files: int) -> Dict[str, Any]:
    return {
        "folder": folder,
        "num_files": num_files,
        "unexpected_columns": [],
        "new_columns_added": 0,
        "skipped": False,
        "total_rows": 0,
        "total_columns": 0,
        "error": None
    }
