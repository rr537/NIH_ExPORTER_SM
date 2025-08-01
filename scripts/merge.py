from typing import Dict, Optional, Union, Tuple, Any
import pandas as pd
import logging
 
def append_dataframes_by_folder(
    config: dict,
    dataframes: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, Dict[str, Any]]]:
    """
    Appends all DataFrames within each folder into a single stacked DataFrame.
    Also summarizes column consistency and row/column statistics.
    Returns both the appended DataFrames and the summary dictionary.
    """

    appended_dataframes = {}
    appended_summary = {}
    force_append = config.get("force_append", False)

    for folder, file_dfs in dataframes.items():
        summary = {
            "folder": folder,
            "num_files": len(file_dfs),
            "unexpected_columns": [],
            "new_columns_added": 0,
            "skipped": False,
            "total_rows": 0,
            "total_columns": 0,
            "error": None
        }

        try:
            column_sets = [set(df.columns) for df in file_dfs.values()]
            common_columns = set.intersection(*column_sets)
            all_columns = set.union(*column_sets)
            unexpected_columns = sorted(all_columns - common_columns)

            summary["unexpected_columns"] = unexpected_columns
            summary["new_columns_added"] = len(unexpected_columns)

            if unexpected_columns and not force_append:
                logger.warning(
                    f" Folder '{folder}' has column mismatches across files. "
                    f"{len(unexpected_columns)} unexpected column(s): {unexpected_columns}"
                )
                logger.info(f" Skipping append for '{folder}' due to column mismatch.")
                summary["skipped"] = True
                appended_summary[folder] = summary
                continue

            appended_df = pd.concat(list(file_dfs.values()), ignore_index=True)
            appended_dataframes[folder] = appended_df

            summary["total_rows"] = appended_df.shape[0]
            summary["total_columns"] = appended_df.shape[1]

            logger.info(
                f" Appended {summary['num_files']} files in folder '{folder}': "
                f"{summary['total_rows']:,} rows, {summary['total_columns']:,} columns."
            )

        except Exception as e:
            logger.error(f" Error appending folder '{folder}': {str(e)}", exc_info=True)
            summary["error"] = str(e)

        appended_summary[folder] = summary

    return appended_dataframes, appended_summary


def remove_true_duplicates_from_df(
    df: pd.DataFrame,
    logger: logging.Logger,
    label: str = "Aggregate Output",
) -> Tuple[pd.DataFrame, int]:
    """
    Detects and removes fully duplicated rows from a single aggregate dataframe.
    Outputs a summary CSV of duplication metrics:
    - total_duplicates
    - extra_duplicates
    - unique_duplicate_rows

    Args:
        df: Input DataFrame.
        logger: Logger instance.
        label: Custom label for logging context (default: 'Aggregated Output').
        summary_csv_path: Path to save the deduplication summary CSV.

    Returns:
        Tuple of cleaned DataFrame and duplicate count.
    """

    logger.info(f" Checking for true duplicates in '{label}'...")

    dedupe_df = df.copy()

    # Convert list-like columns to hashable tuples
    for col in dedupe_df.columns:
        if dedupe_df[col].apply(lambda x: isinstance(x, list)).any():
            dedupe_df[col] = dedupe_df[col].map(lambda x: tuple(x) if isinstance(x, list) else x)

    # Compute all duplicate stats
    duplicates_all = dedupe_df.duplicated(keep=False)
    duplicates_extra = dedupe_df.duplicated(keep='first')
    distinct_duplicate_rows = dedupe_df[duplicates_all].drop_duplicates()

    total_duplicates = duplicates_all.sum()
    extra_duplicates = duplicates_extra.sum()
    unique_duplicate_rows = distinct_duplicate_rows.shape[0]

    # Summary metrics
    dedupe_summary_dict = {
        label: {
            "unique_duplicate_rows": int(unique_duplicate_rows),
            "total_duplicates": int(total_duplicates),
            "extra_duplicates": int(extra_duplicates)
        }
    }

    logger.info(
        f"{unique_duplicate_rows:,} unique duplicates, "
        f"{total_duplicates:,} total, {extra_duplicates:,} extra."
    )

    # Filter out duplicates
    if total_duplicates > 0:
        logger.info(f" Found {total_duplicates:,} duplicate rows in '{label}'. Showing sample...")
        logger.debug(dedupe_df[duplicates_all].head())
        dedupe_df = dedupe_df.drop_duplicates() # Remove all duplicates, keeping the first occurrence

        logger.info(f" Duplicates removed — new shape: {dedupe_df.shape[0]:,} rows × {dedupe_df.shape[1]:,} columns")
    else:
        logger.info(f" No duplicate rows found in '{label}'.")

    return dedupe_df, dedupe_summary_dict


def merge_linked_dataframes(
    dataframes: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None,
) -> Union[Dict[str, pd.DataFrame], pd.DataFrame]:
    """
    Merges semantically linked data sources:
    - PRJ + PRJABS → on 'APPLICATION_ID'

    Args:
        dataframes: Dict of cleaned folder → appended DataFrame
        logger: Optional logger

    Returns:
        Dict of merged dataframes OR single DataFrame 
    """
    # 🧩 Grab sources
    prj = dataframes.get("PRJ")
    prjabs = dataframes.get("PRJABS")

    # 🔍 Validate missing inputs
    missing_sources = []
    if prj is None: missing_sources.append("PRJ")
    if prjabs is None: missing_sources.append("PRJABS")

    if missing_sources and logger:
        logger.warning(f" Missing required merge sources: {missing_sources}")

    linked_dict = {}
    linked_summary_dict = {}

    # 🔗 Merge PRJ + PRJABS
    if prj is not None and prjabs is not None:
        try:
            # Ensure APPLICATION_ID is string and stripped for consistency
            prj["APPLICATION_ID"] = prj["APPLICATION_ID"].astype(str).str.strip().str.upper()
            prjabs["APPLICATION_ID"] = prjabs["APPLICATION_ID"].astype(str).str.strip().str.upper()

            # Record pre-merge dimensions
            prj_shape = prj.shape
            prjabs_shape = prjabs.shape

            # ⛓️ Perform merge
            joined = pd.merge(prj, prjabs, on="APPLICATION_ID", how="left")

            # Record post-merge dimensions
            joined_shape = joined.shape
            rows_added = joined_shape[0] - prj_shape[0]
            cols_added = joined_shape[1] - prj_shape[1]

            # 🧮 Fill summary
            linked_summary_dict["PRJ_PRJABS"] = {
                "PRJ": {"rows": prj_shape[0], "cols": prj_shape[1]},
                "PRJABS": {"rows": prjabs_shape[0], "cols": prjabs_shape[1]},
                "merged": {"rows": joined_shape[0], "cols": joined_shape[1]},
                "changes": {"rows_added": rows_added, "cols_added": cols_added},
            }

            linked_dict["PRJ_PRJABS"] = joined
            if logger:
                logger.info(f" Merged PRJ + PRJABS: {joined_shape[0]:,} rows × {joined_shape[1]:,} columns")
        except Exception as e:
            if logger:
                logger.error(" Failed merging PRJ and PRJABS", exc_info=True)

    return linked_dict, linked_summary_dict
