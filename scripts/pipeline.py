from scripts.config_loader import load_config
from scripts.loader import load_dataframes
from scripts.logger import configure_logger
from scripts.preprocessing import clean_dataframes
from scripts.merge import append_dataframes_by_folder, merge_linked_dataframes
from scripts.aggregation import aggregate_project_outputs
from scripts.keywords import prepare_keywords
from scripts.enrichment import enrich_with_keyword_metrics
import pandas as pd
import json
from pathlib import Path
import logging
from typing import Dict, Tuple
from scripts.training import (
    create_ml_training_df,
    export_training_dataframe
)

def construct_ml_dataframe(
    config: Dict,
    cleaned_data: Dict[str, Dict[str, pd.DataFrame]],
    logger: logging.Logger
) -> pd.DataFrame:

    appended = append_dataframes_by_folder(config, cleaned_data, logger)
    linked = merge_linked_dataframes(appended, logger)
    ml_df = aggregate_project_outputs(linked, appended, logger)

    if ml_df.empty:
        logger.warning(" ML DataFrame construction failed — received empty output.")
    else:
        logger.info(f" ML DataFrame ready: {ml_df.shape[0]:,} rows × {ml_df.shape[1]:,} columns")

    return ml_df

def full_ml_pipeline(config_path: str) -> pd.DataFrame:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    raw = load_dataframes(config_path)
    cleaned = clean_dataframes(config, raw, logger)
    ml_df = construct_ml_dataframe(config, cleaned, logger)

    return ml_df

def full_enrichment_pipeline(config_path: str, remove_stopwords: bool = False) -> pd.DataFrame:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    raw = load_dataframes(config_path, logger)
    cleaned = clean_dataframes(config, raw, logger)
    ml_df = construct_ml_dataframe(config, cleaned, logger)

    treatments, diseases = prepare_keywords(config, logger, remove_stopwords=remove_stopwords)
    enriched_df = enrich_with_keyword_metrics(ml_df, config, treatments, diseases, logger)

    return enriched_df

def finalize_training_dataset(config_path: str, remove_stopwords: bool = False) -> None:
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 🔃 Full pipeline logic in-place for traceability
    raw = load_dataframes(config_path, logger)
    cleaned = clean_dataframes(config, raw, logger)
    appended_df = append_dataframes_by_folder(config, cleaned, logger)
    appended_df_combined = pd.concat(list(appended_df.values()), ignore_index=True)
    linked_df = merge_linked_dataframes(appended_df, logger, flatten = False)
 
    # Check if linked_df is empty
    if linked_df.empty or linked_df.shape[0] == 0:
        logger.warning(" Linked dataset is empty — stopping pipeline.")
        return

    ml_df = aggregate_project_outputs(linked_df, appended_df, logger)

    treatments, diseases = prepare_keywords(config, logger, remove_stopwords)
    enriched_df = enrich_with_keyword_metrics(ml_df, config, treatments, diseases, logger)

    MLdf, dropped_df = create_ml_training_df(enriched_df, config, logger)

    export_training_dataframe(MLdf, config, logger, filename="MLdf_training.csv")
    export_training_dataframe(dropped_df, config, logger, filename="MLdf_dropped.csv")

    # If 'flagged' column is missing, set defaults
    if 'flagged' not in enriched_df.columns:
        logger.warning(" No 'flagged' column found in enriched DataFrame — skipping keyword summary.")
        keyword_total = 0
        top_keywords = {}
    else:
        keyword_total = enriched_df['flagged'].str.len().sum()
        top_keywords = enriched_df['flagged'].explode().value_counts().head(5).to_dict()

    # 📊 Summary JSON construction
    summary_stats = {
        "initial_load": {
            "folders_loaded": list(raw.keys()),
            "file_count": sum(len(folder) for folder in raw.values()),
            "total_raw_rows": sum(df.shape[0] for folder in raw.values() for df in folder.values())
        },
        "preprocessing": {
            "cleaned_rows": sum(df.shape[0] for folder in cleaned.values() for df in folder.values()),
            "columns_dropped": {k: len(v) for k, v in config.get("drop_col_header_map", {}).items()}
        },
        "appended": {
            "total_rows": appended_df_combined.shape[0]
        },
        "linked": {
            "total_rows_after_merge": linked_df.shape[0]
        },
        "aggregated": {
            "ml_df_rows": ml_df.shape[0],
            "ml_df_columns": ml_df.shape[1]
        },
        "enrichment": {
            "keyword_total": keyword_total,
            "top_keywords": top_keywords
        },
        "training_export": {
            "ml_training_rows": MLdf.shape[0],
            "dropped_rows": dropped_df.shape[0]
        }
    }

    output_dir = Path(config.get("output_dir", "results")).resolve()
    summary_path = output_dir / "summary.json"

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary_stats, f, indent=2)

    logger.info(f" Summary saved to: {summary_path}")
