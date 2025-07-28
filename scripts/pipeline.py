import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from scripts.config_loader import load_config
from scripts.loader import load_dataframes
from scripts.logger import configure_logger
from scripts.preprocessing import validate_csv_headers_from_df, drop_specified_columns_from_df, rename_dataframe_columns
from scripts.merge import append_dataframes_by_folder, remove_true_duplicates_from_df, merge_linked_dataframes
from scripts.aggregation import aggregate_project_outputs
from scripts.keywords import prepare_keywords
from scripts.enrichment import enrich_with_keyword_metrics
import pandas as pd
import json
from pathlib import Path
import logging
from typing import Dict, Tuple, List, Optional 
from scripts.training import create_ml_training_df, export_training_dataframe
from datetime import datetime
import yaml

# Load configuration
# Assuming config.yaml is in the same directory as this script
config_path = "config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

dedup_summary_path = config.get("paths", {}).get("dedup_summary_csv_path", "results/duplicate_summary.csv")

# Pipeline to construct a single ML Dataframe with study outcome metrics (publication, patent, clinical study counts)
def full_ml_pipeline(config_path: str, logger: Optional[logging.Logger] = None) -> pd.DataFrame:
    config = load_config(config_path)
    if logger is None:
        logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    raw_dict = load_dataframes(config_path, logger)
    rename_dict, rename_log = rename_dataframe_columns(config, raw_dict, logger)
    appended_dict = append_dataframes_by_folder(config, rename_dict, logger)

    linked_dict = merge_linked_dataframes(appended_dict, logger)
    aggregate_df, outcomes_dedup_summary = aggregate_project_outputs(linked_dict, appended_dict, logger)
    dedup_df , df_dedup_summary = remove_true_duplicates_from_df(aggregate_df, logger)

    dedup_summary = pd.concat([outcomes_dedup_summary, df_dedup_summary], ignore_index=True)
    dedup_summary.to_csv(dedup_summary_path, index=False)
 
    if dedup_df.empty:
        logger.warning(" ML DataFrame construction failed — received empty output.")
    else:
        logger.info(f" ML DataFrame ready: {dedup_df.shape[0]:,} rows × {dedup_df.shape[1]:,} columns")

    return dedup_df

# Pipeline to construct a single ML Dataframe with study outcome metrics + enrich with additional keyword information. Includes keyword counts and flagging
def full_enrichment_pipeline(config_path: str, remove_stopwords: bool = False) -> pd.DataFrame:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    dedup_df = full_ml_pipeline(config_path, logger)

    treatments, diseases = prepare_keywords(config, logger, remove_stopwords=remove_stopwords)
    enriched_df = enrich_with_keyword_metrics(dedup_df, config, treatments, diseases, logger)

    return enriched_df

# Full pipeline to finalize the training dataset, includes study outcome metrics + keyword enrichment + finalization steps (dropping unnecessary columns, removing zero-count rows, and exporting the final DataFrames
def finalize_training_dataset(config_path: str, remove_stopwords: bool = False) -> None:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 🔃 Full pipeline logic in-place for traceability
    raw_dict = load_dataframes(config_path, logger)
    rename_dict, rename_log = rename_dataframe_columns(config, raw_dict, logger)
    appended_dict = append_dataframes_by_folder(config, rename_dict, logger)

    linked_dict = merge_linked_dataframes(appended_dict, logger)
    aggregate_df, outcomes_dedup_summary = aggregate_project_outputs(linked_dict, appended_dict, logger)
    dedup_df , df_dedup_summary = remove_true_duplicates_from_df(aggregate_df, logger)

    dedup_summary = pd.concat([outcomes_dedup_summary, df_dedup_summary], ignore_index=True)
    dedup_summary.to_csv(dedup_summary_path, index=False)

    if dedup_df.empty:
        logger.warning(" ML DataFrame construction failed — received empty output.")
    else:
        logger.info(f" ML DataFrame ready: {dedup_df.shape[0]:,} rows × {dedup_df.shape[1]:,} columns")

    treatments, diseases = prepare_keywords(config, logger, remove_stopwords=remove_stopwords)
    enriched_df = enrich_with_keyword_metrics(dedup_df, config, treatments, diseases, logger)

    MLdf, dropped_df = create_ml_training_df(enriched_df, config, logger)

    # 📊 Flatten just for summary
    appended_df_combined = pd.concat(list(appended_dict.values()), ignore_index=True)
    linked_df_summary = pd.concat(list(linked_dict.values()), ignore_index=True)

    # 📝 Export the final ML DataFrames
    export_training_dataframe(MLdf, config, logger, filename="MLdf_training.csv")
    export_training_dataframe(dropped_df, config, logger, filename="MLdf_dropped.csv")

    # 🧠 If 'flagged' column is missing, use defaults
    if 'flagged' not in MLdf.columns:
        logger.warning(" No 'flagged' column found in enriched DataFrame — skipping keyword summary.")
        keyword_total = 0
        top_keywords = {}
    else:
        keyword_total = MLdf['flagged'].str.len().sum()
        top_keywords = MLdf['flagged'].explode().value_counts().head(5).to_dict()

    # 📊 Build summary report
    summary_stats = {
        "initial_load": {
            "folders_loaded": list(raw_dict.keys()),

            "file_count": int(sum(len(folder) for folder in raw_dict.values())),

            "total_raw_rows": int(sum(int(df.shape[0]) for folder in raw_dict.values() for df in folder.values()))
        },
        "preprocessing": {
            "columns_renamed": rename_log
        },
        "appended": {
            "total_rows": int(appended_df_combined.shape[0]),
            "total_columns": int(appended_df_combined.shape[1]),
        },
        "linked": {
            "total_rows_after_merge": int(linked_df_summary.shape[0]),
            "merged_sources": list(linked_df_summary.keys()),
            "total_columns_after_merge": int(linked_df_summary.shape[1])
        },
        "deduplication": {
            row["category"]: {
                "unique_duplicate_rows": int(row["unique_duplicate_rows"]),
                "total_duplicates": int(row["total_duplicates"]),
                "extra_duplicates": int(row["extra_duplicates"])
            }
            for _, row in df_dedup_summary.iterrows()
        },
        "enrichment": {
            "keyword_total": int(keyword_total),
            "top_keywords": {k: int(v) for k, v in top_keywords.items()}
        },
        "training_export": {
            "ml_training_rows": int(MLdf.shape[0]),
            "ml_training_columns": int(MLdf.shape[1]),
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

