import sys
import os
from pathlib import Path
import json
# Add project root to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

import argparse
import pandas as pd
from typing import Dict, Tuple, List, Optional, Any  
from scripts.config_loader import load_config
from scripts.logger import configure_logger
from scripts.loader import load_dataframes
from scripts.pipeline import preprocess_pipeline, metrics_pipeline, keywords_pipeline, finalize_pipeline
from scripts.keywords import load_keywords
from scripts.enrichment import enrich_with_keyword_metrics
from scripts.loader import (validate_data_sources, validate_folder_path)
from scripts.reporting import build_summary


def preprocess(
    config_path: str, 
    output_path: str, 
    summary_path: str
    )-> Tuple[Dict[str, pd.DataFrame], List[str], Dict[str, Any]]:
    
    # 1. Load configuration and set up logger
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 2. Resolve output directory
    if output_path is None:
        output_path = Path(config.get("preprocessed_dir", "results/preprocessed")).resolve()
    else:
        output_path = Path(output_path).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    # 3. Validate config paths and data sources
    validate_folder_path(config_path, logger)
    validate_data_sources(config_path, logger)

    # 4. Run preprocessing pipeline
    preprocess_dict, rename_summary, load_summary, appended_summary = preprocess_pipeline(config=config, logger=logger)

    # 5. Export each appended DataFrame to pickle
    print(" Exporting the following keys:", list(preprocess_dict.keys()))
    for name, df in preprocess_dict.items():
        path = output_path / f"{name}.pkl"
        df.to_pickle(path)
        print(f" Saved {name}.pkl to {path}")

    # 6. Combine data for summary statistics 
    preprocess_combined_df = pd.concat(list(preprocess_dict.values()), ignore_index=True)

    # 7.  Prepare metadata for build_summary() 
    preprocess_metadata = {
        "load_summary": [
            {
                "folder": folder,
                "file_count": stats["file_count"],
                "total_raw_rows": stats["total_rows"],
                "total_memory": f"{stats['total_memory']:.2f} MB",
                **({
                    "appended_rows": appended_summary[folder].get("total_rows", 0),
                    "appended_columns": appended_summary[folder].get("total_columns", 0),
                    "new_columns_added": appended_summary[folder].get("new_columns_added", 0),
                    "unexpected_columns": appended_summary[folder].get("unexpected_columns", []),
                    "skipped_due_to_mismatch": appended_summary[folder].get("skipped", False),
                    "append_error": appended_summary[folder].get("error")
                } if folder in appended_summary else {})
            }
            for folder, stats in load_summary.items()
        ],
        "rename_summary": rename_summary,
        "total_rows": int(preprocess_combined_df.shape[0]),
        "total_columns": int(preprocess_combined_df.shape[1])
    }

    # 8. Build summary statistics
    summary = build_summary(preprocess_stats=preprocess_metadata)

    # 9. Export summary to JSON
    if summary_path is None:
        summary_path = output_path / "preprocessing_summary.json"
    else:
        summary_path = Path(summary_path).resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info(f" Preprocessing summary exported to: {summary_path}")

    return preprocess_dict, preprocess_metadata

def metrics(config_path: str, output_path: str, summary_path: str):
    # 1. Load configuration and set up logger
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 2. Resolve output directory
    if output_path is None:
        output_path = Path(config.get("metrics_dir", "results/study_metrics")).resolve()
    else:
        output_path = Path(output_path).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    # 3. Load preprocessed DataFrames
    logger.info(" Loading preprocessed DataFrames...")
    input_dir = Path(config.get("preprocessed_dir", "results/preprocessed")).resolve()
    pickle_paths = sorted(input_dir.glob("*.pkl"))

    # Load all DataFrames from pickle files into a dict
    appended_dict = {
            p.stem: pd.read_pickle(p)
            for p in pickle_paths
        }

    logger.info(f" Loaded {len(appended_dict)} DataFrames:")
    for key, df in appended_dict.items():
        logger.info(f"  └─ {key}: {df.shape[0]:,} rows × {df.shape[1]:,} columns")

    # 4. Run metrics pipeline
    metrics_df, linked_summary_dict, aggregate_outcomes_summary_dict, dedupe_summary_dict = metrics_pipeline(appended_dict ,logger=logger)

    # 5. Export metrics DataFrame to CSV
    metrics_path = output_path / "metrics.csv"
    metrics_df.to_csv(metrics_path, index=False)
    logger.info(f" Metrics DataFrame saved to: {output_path}")

    # 6.  Prepare metadata for build_summary()
    metrics_metadata = {    "linked_summary": linked_summary_dict,
        "aggregate_outcomes_summary": aggregate_outcomes_summary_dict,
        "dedupe_summary": dedupe_summary_dict,
        "total_rows": int(metrics_df.shape[0]),
        "total_columns": int(metrics_df.shape[1])
    }

    # 7. Build summary statistics
    summary = build_summary(metrics_stats=metrics_metadata)

    # 8. Export summary to JSON
    if summary_path is None:
        summary_path = output_path / "metrics_summary.json"
    else:
        summary_path = Path(summary_path).resolve()
    summary_path.parent.mkdir(parents=True, exist_ok=True)

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info(f" Preprocessing summary exported to: {summary_path}")

    return metrics_df, metrics_metadata


def keywords(config_path: str, output_path: str, summary_path: str, remove_stopwords: bool = False):
    # 1. Load configuration and set up logger
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 2. Resolve output directory
    if output_path is None:
        output_path = Path(config.get("keywords_dir", "results/keywords")).resolve()
    else:
        output_path = Path(output_path).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    # 3. Load metrics DataFrames 
    logger.info(" Loading metrics DataFrame...")
    input_dir = Path(config.get("metrics_dir", "results/study_metrics")).resolve()
    metrics_path = input_dir / "metrics.csv"
    metrics_df = pd.read_csv(metrics_path, low_memory=False)
    logger.info(f" Loaded metrics DataFrame: {metrics_df.shape[0]:,} rows × {metrics_df.shape[1]:,} columns")
    
    # 4. Run keywords pipeline
    keywords_df, keywords_summary, enrichment_summary = keywords_pipeline(metrics_df, logger=logger, remove_stopwords=remove_stopwords)

    # 5. Export keywords DataFrame to CSV
    keywords_path = output_path / "keywords.csv"
    keywords_df.to_csv(keywords_path, index=False)
    logger.info(f" Keyword-enriched DataFrame saved to: {keywords_path}")

    # 6.  Prepare metadata for build_summary()
    keywords_metadata = { "keywords_summary": keywords_summary,
        "enrichment_summary": enrichment_summary,
        "total_rows": int(keywords_df.shape[0]),
        "total_columns": int(keywords_df.shape[1])
    }
    # 7. Build summary statistics
    summary = build_summary(keywords_stats=keywords_metadata)

    # 8. Export summary to JSON
    if summary_path is None:
        summary_path = output_path / "keywords_summary.json"
    else:
        summary_path = Path(summary_path).resolve()

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info(f" Keywords summary exported to: {summary_path}")

    return keywords_df, keywords_metadata

def finalize (config_path: str, output_path: str, summary_path: str, drop_rows: bool = False):
    # 1. Load configuration and set up logger
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 2. Resolve output directory
    if output_path is None:
        output_path = Path(config.get("finalize_dir", "results/finalize")).resolve()
    else:
        output_path = Path(output_path).resolve()
    output_path.mkdir(parents=True, exist_ok=True)

    # 3. Load keywords DataFrames 
    logger.info(" Loading keywords-enriched DataFrame...")
    input_dir = Path(config.get("keywords_dir", "results/keywords")).resolve()
    keywords_path = input_dir / "keywords.csv"
    keywords_df = pd.read_csv(keywords_path, low_memory=False)
    logger.info(f" Loaded keywords DataFrame: {keywords_df.shape[0]:,} rows × {keywords_df.shape[1]:,} columns")

    # 4. Run finalize pipeline
    MLdf, MLdf_dropped, finalize_summary = finalize_pipeline(keywords_df, config=config, logger=logger, drop_rows=drop_rows)

    # 5. Export finalized data to CSV
    finalize_path = output_path / "finalized.csv"
    MLdf.to_csv(finalize_path, index=False)
    logger.info(f" Finalized DataFrame saved to: {finalize_path}")

    if drop_rows:
        dropped_path = output_path / "dropped_rows.csv"
        MLdf_dropped.to_csv(dropped_path, index=False)
        logger.info(f" Dropped rows saved to: {dropped_path}")
    
    # 6. Prepare metadata for build_summary()
    finalize_metadata = {
        "finalize_summary": finalize_summary,
        "total_rows": int(MLdf.shape[0]),
        "total_columns": int(MLdf.shape[1]),
        "dropped_rows": int(MLdf_dropped.shape[0]) if drop_rows else 0
    }
    # 7. Build summary statistics
    summary = build_summary(finalize_stats=finalize_metadata)

    # 8. Export summary to JSON
    if summary_path is None:
        summary_path = output_path / "finalize_summary.json"
    else:
        summary_path = Path(summary_path).resolve()

    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info(f" Finalized dataset summary exported to: {summary_path}")

    return MLdf, finalize_metadata

def main():
    parser = argparse.ArgumentParser(description="NIH ExPORTER CLI")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 👷 Preprocessing step
    preprocess_parser = subparsers.add_parser("preprocess", help="Rename and append raw CSVs")
    preprocess_parser.add_argument("--config", required=True, help="Path to config.yaml")
    preprocess_parser.add_argument("--output", help="Path to output directory for pickled DataFrames")
    preprocess_parser.add_argument("--summary-json", help="Optional path to export preprocessing summary as JSON", required=False)

    # Metrics step
    metrics_parser = subparsers.add_parser("metrics", help="Aggregate project outcomes into a single dataset")
    metrics_parser.add_argument("--config", required=True, help="Path to config.yaml")
    metrics_parser.add_argument("--output", help="Path to output directory for aggregated outcomes dataset")
    metrics_parser.add_argument("--summary-json", help="Optional path to export metrics summary as JSON", required=False)

    # ✨ Keyword enrichment step
    keyword_parser = subparsers.add_parser("keywords", help="Count and flag keywords in project text related columns")
    keyword_parser.add_argument("--config", required=True, help="Path to config.yaml")
    keyword_parser.add_argument("--output", help="Path to output CSV")
    keyword_parser.add_argument("--summary-json", help="Optional path to export keyword summary as JSON", required=False)
    keyword_parser.add_argument("--stopwords", action="store_true", help="Remove English stopwords during keyword enrichment")

    # 📦 Finalize training dataset step
    finalize_parser = subparsers.add_parser("finalize", help="Filter out unnecessary columns and prepare final training dataset")
    finalize_parser.add_argument("--config", required=True, help="Path to config.yaml")
    finalize_parser.add_argument("--output", help="Path to output CSV")
    finalize_parser.add_argument("--summary-json", help="Optional path to export training dataset summary as JSON", required=False)
    finalize_parser.add_argument("--drop-output", action="store_true", help="Export dropped rows")

    args = parser.parse_args()
    print(f" Parsed command: {args.command}")

    if args.command == "preprocess":
        preprocess_dict, preprocess_metadata = preprocess(config_path=args.config, output_path=args.output, summary_path=args.summary_json)

    elif args.command == "metrics":
        metrics_df, metrics_metadata = metrics(config_path=args.config, output_path=args.output, summary_path=args.summary_json)

    elif args.command == "keywords":
        keywords_df, keywords_metadata = keywords(args.config, args.output, summary_path=args.summary_json, remove_stopwords=args.stopwords)
    
    elif args.command == "finalize":
        MLdf, finalize_metadata = finalize(args.config, args.output, summary_path=args.summary_json, drop_rows = args.drop_output)

if __name__ == "__main__":
    main()
