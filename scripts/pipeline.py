from scripts.config_loader import load_config
from scripts.loader import load_dataframes
from scripts.logger import configure_logger
from scripts.preprocessing import validate_csv_headers, drop_specified_columns, rename_dataframe_columns
from scripts.merge import append_dataframes_by_folder, remove_true_duplicates, merge_linked_dataframes
from scripts.aggregation import aggregate_project_outputs
from scripts.keywords import prepare_keywords
from scripts.enrichment import enrich_with_keyword_metrics
import pandas as pd
import json
from pathlib import Path
import logging
from typing import Dict, Tuple, List
from scripts.training import create_ml_training_df, export_training_dataframe
from datetime import datetime

# Helper function to validate CSV headers, drop specified columns, and rename columns
def clean_dataframes(
    config: dict,
    dataframes: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> Tuple[
    Dict[str, Dict[str, pd.DataFrame]],  # cleaned data
    Dict[str, Dict[str, List[str]]],     # column summary per folder
    int                                  # total_duplicates_removed
    ]:
    """
    Cleans loaded dataframes by validating headers, dropping configured columns,
    renaming columns using config rules, and removing true duplicates.
    Logs a summary of duplicates removed across all files.
    """
    logger.info(" Starting header validation...")
    validate_csv_headers(config, dataframes, logger)

    logger.info(" Dropping specified columns...")
    drop_specified_columns(config, dataframes, logger)

    logger.info(" Renaming columns...")
    rename_dataframe_columns(config, dataframes, logger)

    logger.info(" Removing true duplicates...")
    total_removed = 0

    # Remove duplicates by iterating through each folder and its DataFrames
    for folder, files in dataframes.items():
        for name, df in files.items():
            cleaned, removed = remove_true_duplicates(df, logger, folder, name)
            dataframes[folder][name] = cleaned
            total_removed += removed

    # Collect column summary for each folder
    column_summary = {}
   
    for folder in dataframes:
        dropped = config.get("drop_col_header_map", {}).get(folder, [])
        renamed = list(config.get("rename_columns_map", {}).keys())
        column_summary[folder] = {
            "dropped_columns": dropped,
            "renamed_columns": renamed
        }

    logger.info(f" Data cleaning complete. Total duplicates removed across all files: {total_removed:,}")

    return dataframes, column_summary, total_removed

# Helper function to construct the ML DataFrame
def construct_ml_dataframe(
    config: Dict,
    cleaned_data: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> pd.DataFrame:

    appended_df = append_dataframes_by_folder(config, cleaned_data, logger)
    deduped_df, _ = deduplicate_appended_dataframes(appended_df, logger)
    linked = merge_linked_dataframes(deduped_df, logger)
    ml_df = aggregate_project_outputs(linked, deduped_df, logger)

    if ml_df.empty:
        logger.warning(" ML DataFrame construction failed — received empty output.")
    else:
        logger.info(f" ML DataFrame ready: {ml_df.shape[0]:,} rows × {ml_df.shape[1]:,} columns")

    return ml_df

# Helper function to deduplicate appended DataFrames
def deduplicate_appended_dataframes(
    appended_df: Dict[str, pd.DataFrame],
    logger: logging.Logger
) -> Tuple[Dict[str, pd.DataFrame], int]:
    deduped_df = {}
    total_duplicates_removed = 0

    for folder_name, df in appended_df.items():
        cleaned_df, num_removed = remove_true_duplicates(df, logger, folder_name)
        deduped_df[folder_name] = cleaned_df
        total_duplicates_removed += num_removed

    return deduped_df, total_duplicates_removed

# Pipeline to construct a single ML Dataframe with study outcome metrics (publication, patent, clinical study counts)
def full_ml_pipeline(config_path: str) -> pd.DataFrame:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    raw = load_dataframes(config_path)
    cleaned, column_summary, total_dup_removed = clean_dataframes(config, raw, logger)
    ml_df = construct_ml_dataframe(config, cleaned, logger)

    return ml_df

# Pipeline to construct a single ML Dataframe with study outcome metrics + enrich with additional keyword information. Includes keyword counts and flagging
def full_enrichment_pipeline(config_path: str, remove_stopwords: bool = False) -> pd.DataFrame:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    raw = load_dataframes(config_path, logger)
    cleaned, column_summary, total_dup_removed = clean_dataframes(config, raw, logger)
    ml_df = construct_ml_dataframe(config, cleaned, logger)

    treatments, diseases = prepare_keywords(config, logger, remove_stopwords=remove_stopwords)
    enriched_df = enrich_with_keyword_metrics(ml_df, config, treatments, diseases, logger)

    return enriched_df

# Full pipeline to finalize the training dataset, includes study outcome metrics + keyword enrichment + finalization steps (dropping unnecessary columns, removing zero-count rows, and exporting the final DataFrames
def finalize_training_dataset(config_path: str, remove_stopwords: bool = False) -> None:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 🔃 Full pipeline logic in-place for traceability
    raw = load_dataframes(config_path, logger)
    cleaned, column_summary, total_dup_removed = clean_dataframes(config, raw, logger)

    appended_df = append_dataframes_by_folder(config, cleaned, logger)
    deduped_df, total_duplicates_removed = deduplicate_appended_dataframes(appended_df, logger)
    linked_df_dict = merge_linked_dataframes(deduped_df, logger)
    ml_df = aggregate_project_outputs(linked_df_dict, deduped_df, logger)

    treatments, diseases = prepare_keywords(config, logger, remove_stopwords)
    enriched_df = enrich_with_keyword_metrics(ml_df, config, treatments, diseases, logger)
    MLdf, dropped_df = create_ml_training_df(enriched_df, config, logger)

    # 📊 Flatten just for summary
    appended_df_combined = pd.concat(list(appended_df.values()), ignore_index=True)
    deduped_df_combined = pd.concat(list(deduped_df.values()), ignore_index=True) 
    linked_df_summary = pd.concat(list(linked_df_dict.values()), ignore_index=True)

    # 📝 Export the final ML DataFrames
    export_training_dataframe(MLdf, config, logger, filename="MLdf_training.csv")
    export_training_dataframe(dropped_df, config, logger, filename="MLdf_dropped.csv")

    # 🧠 If 'flagged' column is missing, use defaults
    if 'flagged' not in enriched_df.columns:
        logger.warning(" No 'flagged' column found in enriched DataFrame — skipping keyword summary.")
        keyword_total = 0
        top_keywords = {}
    else:
        keyword_total = enriched_df['flagged'].str.len().sum()
        top_keywords = enriched_df['flagged'].explode().value_counts().head(5).to_dict()

    # 📊 Build summary report
    summary_stats = {
        "initial_load": {
            "folders_loaded": list(raw.keys()),
            "file_count": int(sum(len(folder) for folder in raw.values())),
            "total_raw_rows": int(sum(int(df.shape[0]) for folder in raw.values() for df in folder.values()))
        },
        "preprocessing": {
            "cleaned_rows": int(sum(df.shape[0] for folder in cleaned.values() for df in folder.values())),
            "columns_dropped": {folder: column_summary[folder]["dropped_columns"] for folder in column_summary},
            "columns_renamed": {folder: column_summary[folder]["renamed_columns"] for folder in column_summary},
            "duplicates_removed": int(total_dup_removed)
        },
        "appended": {
            "total_rows": int(appended_df_combined.shape[0]),
            "total_columns": int(appended_df_combined.shape[1]),
        },
        "deduped": {
            "total_rows_after_deduplication": int(deduped_df_combined.shape[0]),
            "duplicates_removed": int(total_duplicates_removed)
        },
        "linked": {
            "total_rows_after_merge": int(linked_df_summary.shape[0]),
            "merged_sources": list(linked_df_dict.keys()),
            "total_columns_after_merge": int(linked_df_summary.shape[1])
        },
        "aggregated": {
            "ml_df_rows": int(ml_df.shape[0]),
            "ml_df_columns": int(ml_df.shape[1])
        },
        "enrichment": {
            "keyword_total": int(keyword_total),
            "top_keywords": {k: int(v) for k, v in top_keywords.items()}
        },
        "training_export": {
            "ml_training_rows": int(MLdf.shape[0]),
            "dropped_rows": int(dropped_df.shape[0])
        },
        "run_metadata": {
            "timestamp": datetime.now().isoformat()
    }
}
    
    # 📁 Export summary JSON
    output_dir = Path(config.get("output_dir", "results")).resolve()
    summary_path = output_dir / "summary.json"

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_stats, f, indent=2)

    logger.info(f" Summary saved to: {summary_path}")

