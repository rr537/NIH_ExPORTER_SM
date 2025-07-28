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

def remove_true_duplicates_from_df(
    df: pd.DataFrame,
    logger: logging.Logger,
    label: str = "Aggregate Output",
    dedup_summary_path: str = "results/duplicate_summary.csv"
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

    df_processed = df.copy()

    # Convert list-like columns to hashable tuples
    for col in df_processed.columns:
        if df_processed[col].apply(lambda x: isinstance(x, list)).any():
            df_processed[col] = df_processed[col].map(lambda x: tuple(x) if isinstance(x, list) else x)

    # Compute all duplicate stats
    duplicates_all = df_processed.duplicated(keep=False)
    duplicates_extra = df_processed.duplicated(keep='first')
    distinct_duplicate_rows = df_processed[duplicates_all].drop_duplicates()

    total_duplicates = duplicates_all.sum()
    extra_duplicates = duplicates_extra.sum()
    unique_duplicate_rows = distinct_duplicate_rows.shape[0]

    # Output summary CSV
    df_dedup_summary = pd.DataFrame([{
        "category": label,
        "unique_duplicate_rows": unique_duplicate_rows,
        "total_duplicates": total_duplicates,
        "extra_duplicates": extra_duplicates
    }])
    # df_dedup_summary.to_csv(dedup_summary_path, index=False)

    logger.info(
        f" Summary written to {dedup_summary_path}: "
        f"{unique_duplicate_rows} unique duplicates, "
        f"{total_duplicates} total, {extra_duplicates} extra."
    )

    # Filter out duplicates
    if total_duplicates > 0:
        logger.info(f" Found {total_duplicates:,} duplicate rows in '{label}'. Showing sample...")
        logger.debug(df_processed[duplicates_all].head())
        df_processed = df_processed.drop_duplicates() # Remove all duplicates, keeping the first occurrence

        logger.info(f" Duplicates removed — new shape: {df_processed.shape[0]:,} rows × {df_processed.shape[1]:,} columns")
    else:
        logger.info(f" No duplicate rows found in '{label}'.")

    return df_processed, df_dedup_summary


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

    merged_outputs = {}

    # 🔗 Merge PRJ + PRJABS
    if prj is not None and prjabs is not None:
        try:
            
            # Ensure APPLICATION_ID is string and stripped for consistency
            prj["APPLICATION_ID"] = prj["APPLICATION_ID"].astype(str).str.strip().str.upper()
            prjabs["APPLICATION_ID"] = prjabs["APPLICATION_ID"].astype(str).str.strip().str.upper()

            joined = pd.merge(prj, prjabs, on="APPLICATION_ID", how="left")
            merged_outputs["PRJ_PRJABS"] = joined
            if logger:
                logger.info(f" Merged PRJ + PRJABS: {joined.shape[0]:,} rows × {joined.shape[1]:,} columns")
        except Exception as e:
            if logger:
                logger.error(" Failed merging PRJ and PRJABS", exc_info=True)

    return merged_outputs 
