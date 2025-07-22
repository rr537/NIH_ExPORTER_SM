from typing import Dict, Optional
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

def merge_linked_dataframes(
    dataframes: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None
) -> Dict[str, pd.DataFrame]:
    """
    Merges semantically linked data sources:
    - PRJ + PRJABS → on 'APPLICATION_ID'
    - PUB + PUBLINK → on 'PMID'

    Returns:
        Dictionary of merged pairs: 'PRJ_PRJABS' and 'PUB_PUBLINK'
    """
    merged_outputs = {}

    # 💡 Merge PRJ + PRJABS
    prj, prjabs = dataframes.get("PRJ"), dataframes.get("PRJABS")
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
    pub, publink = dataframes.get("PUB"), dataframes.get("PUBLINK")
    if pub is not None and publink is not None:
        try:
            joined = pd.merge(pub, publink, on="PMID", how="left")
            merged_outputs["PUB_PUBLINK"] = joined
            if logger:
                logger.info(f" Merged PUB + PUBLINK: {joined.shape[0]:,} rows × {joined.shape[1]:,} columns")
        except Exception as e:
            if logger:
                logger.error(" Failed merging PUB and PUBLINK", exc_info=True)

    return merged_outputs