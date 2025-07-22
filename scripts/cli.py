import sys
import os
from pathlib import Path
# Add project root to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import argparse
import pandas as pd
from scripts.config_loader import load_config
from scripts.logger import configure_logger
from scripts.loader import load_dataframes
from scripts.preprocessing import clean_dataframes
from scripts.pipeline import (
    full_enrichment_pipeline,
    full_ml_pipeline,
    finalize_training_dataset
)
from scripts.keywords import load_keywords
from scripts.enrichment import enrich_with_keyword_metrics
from scripts.loader import (validate_data_sources, validate_folder_path)


def preprocess(config_path: str, output_path: str):
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    validate_folder_path(config_path, logger)
    validate_data_sources(config_path, logger)

    raw_data = load_dataframes(config_path, logger)
    cleaned_data = clean_dataframes(config, raw_data, logger)

    # Combine all cleaned DataFrames across folders
    all_frames = [
        df for folder_dfs in cleaned_data.values()
        for df in folder_dfs.values()
    ]
    unified_df = pd.concat(all_frames, ignore_index=True)
    # 👇 Create directory if missing
    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    unified_df.to_csv(output_path, index=False)
    logger.info(f" Exported cleaned dataset to: {output_path}")

def enrich(config_path: str, output_path: str, remove_stopwords: bool = False):
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    enriched_df = full_enrichment_pipeline(
        config_path=config_path,
        remove_stopwords=remove_stopwords
    )
    enriched_df.to_csv(output_path, index=False)
    logger.info(f" Keyword-enriched ML DataFrame saved to: {output_path}")

def main():
    parser = argparse.ArgumentParser(description="NIH ExPORTER CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 👷 Preprocessing step
    preprocess_parser = subparsers.add_parser("preprocess", help="Clean and concatenate raw CSVs")
    preprocess_parser.add_argument("--config", required=True, help="Path to config.yaml")
    preprocess_parser.add_argument("--output", required=True, help="Path to output CSV")

    # ✨ Enrichment step
    enrich_parser = subparsers.add_parser("enrich", help="Run full ML enrichment pipeline")
    enrich_parser.add_argument("--config", required=True, help="Path to config.yaml")
    enrich_parser.add_argument("--output", required=True, help="Path to output CSV")
    enrich_parser.add_argument("--stopwords", action="store_true", help="Remove English stopwords during keyword enrichment")

    train_parser = subparsers.add_parser("train", help="Run full pipeline and export ML training + dropped CSVs")
    train_parser.add_argument("--config", required=True, help="Path to config.yaml")
    train_parser.add_argument("--stopwords", action="store_true", help="Remove stopwords during keyword enrichment")


    args = parser.parse_args()

    if args.command == "preprocess":
        preprocess(args.config, args.output)

    elif args.command == "enrich":
        enrich(args.config, args.output, remove_stopwords=args.stopwords)
    
    elif args.command == "train":
        finalize_training_dataset(args.config)

if __name__ == "__main__":
    main()
