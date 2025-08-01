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
from scripts.training import create_ml_training_df
from datetime import datetime
import yaml

# Load configuration
# Assuming config.yaml is in the same directory as this script
config_path = "config/config.yaml"
with open(config_path, "r") as f:
    config = yaml.safe_load(f)

dedup_summary_path = config.get("paths", {}).get("dedup_summary_csv_path", "results/duplicate_summary.csv")


# Preprocess pipeline to constructed appended source data, exported as seperate pickle files 
def preprocess_pipeline(config: dict, logger: logging.Logger) -> Dict[str, pd.DataFrame] :    
    raw_dict, load_summary = load_dataframes(config_path, logger)
    rename_dict, rename_summary = rename_dataframe_columns(config, raw_dict, logger)
    appended_dict, appended_summary = append_dataframes_by_folder(config, rename_dict, logger)

    return appended_dict, rename_summary, load_summary, appended_summary

# Pipeline to construct a single ML Dataframe with study outcome metrics (publication, patent, clinical study counts)
def metrics_pipeline(appended_dict: dict, logger: Optional[logging.Logger] = None) -> pd.DataFrame:

    linked_dict, linked_summary_dict = merge_linked_dataframes(appended_dict, logger)
    aggregate_df, aggregate_outcomes_summary_dict = aggregate_project_outputs(linked_dict, appended_dict, logger)
    dedup_df , dedupe_summary_dict = remove_true_duplicates_from_df(aggregate_df, logger)

    return dedup_df, linked_summary_dict, aggregate_outcomes_summary_dict, dedupe_summary_dict

# Pipeline to construct a single ML Dataframe with study outcome metrics + enrich with additional keyword information. Includes keyword counts and flagging
def keywords_pipeline(metrics_df, logger: logging.Logger, remove_stopwords: bool = False) -> pd.DataFrame:

    treatments, diseases, keywords_summary = prepare_keywords(config, logger, remove_stopwords=remove_stopwords)
    keywords_df, enrichment_summary = enrich_with_keyword_metrics(metrics_df, config, treatments, diseases, logger)

    return keywords_df, keywords_summary, enrichment_summary

# Full pipeline to finalize the training dataset, includes study outcome metrics + keyword enrichment + finalization steps (dropping unnecessary columns, removing zero-count rows, and exporting the final DataFrames
def finalize_pipeline(keywords_df, config: dict, logger: logging.Logger, drop_rows: bool = False) -> pd.DataFrame:
    
    MLdf, MLdf_dropped, finalize_summary = create_ml_training_df(keywords_df, config, logger)

    return MLdf, MLdf_dropped, finalize_summary
