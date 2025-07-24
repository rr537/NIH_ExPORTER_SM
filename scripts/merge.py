from typing import Dict, Optional, Union, Tuple
import pandas as pd
import logging

def append_dataframes_by_folder(
    config: dict,
    dataframes: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> Dict[str, pd.DataFrame]:
    """
    Appends all DataFrames within each folder into a single stacked DataFrame.
    """

    appended_dataframes = {}
    force_append = config.get("force_append", False)
    merged_stats = {}

    for folder, file_dfs in dataframes.items():
        try:
            num_files = len(file_dfs)
            row_counts = [len(df) for df in file_dfs.values()]
            total_rows = sum(row_counts)

            column_sets = [set(df.columns) for df in file_dfs.values()]
            common_columns = set.intersection(*column_sets)
            all_columns = set.union(*column_sets)
            unexpected_columns = all_columns - common_columns

            new_columns_added = len(all_columns) - len(common_columns)

            if new_columns_added > 0:
                logger.warning(
                    f" Folder '{folder}' has column mismatches across files. "
                    f"{new_columns_added} unexpected column(s): {sorted(unexpected_columns)}"
                )
                if not force_append:
                    logger.info(f" Skipping append for '{folder}' due to column mismatch.")
                    continue

            appended_df = pd.concat(list(file_dfs.values()), ignore_index=True)
            appended_dataframes[folder] = appended_df

            logger.info(
                f" Appended {num_files} files in folder '{folder}': {appended_df.shape[0]:,} rows, {appended_df.shape[1]:,} columns."
            )

        except Exception as e:
            logger.error(f" Error appending folder '{folder}': {str(e)}", exc_info=True)

    return appended_dataframes

def remove_true_duplicates(
    df: pd.DataFrame,
    logger: logging.Logger,
    folder_name: str,
    file_name: str = None
) -> Tuple[pd.DataFrame, int]:
    """
    Detects and removes fully duplicated rows, including handling list columns via tuple conversion.
    Logs duplicate count and updated shape per folder.

    Returns:
        Cleaned DataFrame and number of duplicates removed.
    """
    df_processed = df.apply(lambda col: col.map(lambda x: tuple(x) if isinstance(x, list) else x))
    duplicates = df_processed.duplicated(keep=False)
    num_duplicates = duplicates.sum()

    label = f"'{file_name}' (folder: '{folder_name}')" if file_name else f"folder '{folder_name}'"

    if num_duplicates > 0:
        logger.info(f" Found {num_duplicates} duplicate rows in {label}. Showing sample...")
        logger.debug(df_processed[duplicates].head())
        df_processed = df_processed[~duplicates]
        logger.info(f" Duplicates removed from {label} — new shape: {df_processed.shape}")
    else:
        logger.info(f" No duplicate rows found in {label}.")

    return df_processed, num_duplicates

def merge_linked_dataframes(
    dataframes: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None,
    flatten: bool = False
) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Merges semantically linked data sources:
    - PRJ + PRJABS → on 'APPLICATION_ID'
    - PUB + PUBLINK → on 'PMID'

    Args:
        dataframes: Dict of cleaned folder → appended DataFrame
        logger: Optional logger
        flatten: If True, returns a single combined DataFrame

    Returns:
        Dict of merged dataframes OR single DataFrame (if flatten=True)
    """
    # 🧩 Grab sources
    prj = dataframes.get("PRJ")
    prjabs = dataframes.get("PRJABS")
    pub = dataframes.get("PUB")
    publink = dataframes.get("PUBLINK")

    # 🔍 Validate missing inputs
    missing_sources = []
    if prj is None: missing_sources.append("PRJ")
    if prjabs is None: missing_sources.append("PRJABS")
    if pub is None: missing_sources.append("PUB")
    if publink is None: missing_sources.append("PUBLINK")

    if missing_sources and logger:
        logger.warning(f" Missing required merge sources: {missing_sources}")

    merged_outputs = {}

    # 🔗 Merge PRJ + PRJABS
    if prj is not None and prjabs is not None:
        try:
            joined = pd.merge(prj, prjabs, on="APPLICATION_ID", how="left")
            merged_outputs["PRJ_PRJABS"] = joined
            if logger:
                logger.info(f" Merged PRJ + PRJABS: {joined.shape[0]:,} rows × {joined.shape[1]:,} columns")
        except Exception as e:
            if logger:
                logger.error(" Failed merging PRJ and PRJABS", exc_info=True)

    # 🔗 Merge PUB + PUBLINK
    if pub is not None and publink is not None:
        try:
            joined = pd.merge(pub, publink, on="PMID", how="left")
            merged_outputs["PUB_PUBLINK"] = joined
            if logger:
                logger.info(f" Merged PUB + PUBLINK: {joined.shape[0]:,} rows × {joined.shape[1]:,} columns")
        except Exception as e:
            if logger:
                logger.error(" Failed merging PUB and PUBLINK", exc_info=True)

    # 📦 Return flattened DataFrame if requested
    if flatten:
        if not merged_outputs:
            if logger:
                logger.warning(" No merged outputs available for flattening. Returning empty DataFrame.")
            return pd.DataFrame()
        return pd.concat(list(merged_outputs.values()), ignore_index=True)

    return merged_outputs  # ✅ Explicit return if flatten=False
