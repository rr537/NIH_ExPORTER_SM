import pandas as pd
from typing import Dict, Tuple, List, Any, Optional
import logging

# Column Renaming
def rename_columns(
    config: dict,
    raw_dict: Dict[str, Dict[str, pd.DataFrame]],
    logger: Optional[logging.Logger] = None
) -> Tuple[Dict[str, Dict[str, pd.DataFrame]], Dict[str, List[str]]]:
   
    """
    Applies renaming rules to all DataFrames using config['rename_columns_map'],
    logging detailed column changes and returning the renamed structure.

    Returns:
        - renamed_dict: DataFrames with updated column names
        - rename_summary: Dict summarizing per-file column changes
    """
    if logger:
        logger.info("Renaming columns using configured rules...")
    rename_map = config.get("rename_columns_map", {})
    
    if not rename_map:
        if logger:
            logger.warning("No renaming rules found in config['rename_columns_map'].")
        return raw_dict, {}

    if logger:
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

            if changes and logger:
                logger.info(f"[{file_name}] Renamed columns: {', '.join(changes)}")
                rename_summary[file_name] = changes

            renamed_dict[folder][file_name] = renamed_df

    return renamed_dict, rename_summary

# Folder-wise DataFrame Appending
def append_dataframes_by_folder(
    config: dict,
    dataframes: Dict[str, Dict[str, pd.DataFrame]],
    logger: Optional[logging.Logger] = None
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, Any]]]:
    """
    Appends all DataFrames within each folder into a single stacked DataFrame.
    Also summarizes column consistency and row/column statistics.
    Returns both the appended DataFrames and the summary dictionary.
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
                if logger:
                    logger.warning(f"Column mismatch in '{folder}': {unexpected}")
                    logger.info(f"Skipping append for '{folder}' due to mismatch.")
                summary["skipped"] = True
                summary_dict[folder] = summary
                continue

            appended_df = pd.concat(list(dfs.values()), ignore_index=True)
            appended_dict[folder] = appended_df

            summary["total_rows"] = appended_df.shape[0]
            summary["total_columns"] = appended_df.shape[1]

            if logger:
                logger.info(
                    f"Appended {summary['num_files']} files in '{folder}': "
                    f"{summary['total_rows']:,} rows, {summary['total_columns']:,} columns."
                )

        except Exception as e:
            summary["error"] = str(e)
            if logger:
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
