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
from scripts.pipeline import preprocess_pipeline, metrics_pipeline, keywords_pipeline, finalize_training_dataset
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

    # 3. Load metrics DataFrames =
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
    keyword_parser = subparsers.add_parser("keywords", help="Run full ML enrichment pipeline")
    keyword_parser.add_argument("--config", required=True, help="Path to config.yaml")
    keyword_parser.add_argument("--output", help="Path to output CSV")
    keyword_parser.add_argument("--summary-json", help="Optional path to export metrics summary as JSON", required=False)
    keyword_parser.add_argument("--stopwords", action="store_true", help="Remove English stopwords during keyword enrichment")

    # 📦 Training step
    train_parser = subparsers.add_parser("train", help="Run full pipeline and export ML training + dropped CSVs")
    train_parser.add_argument("--config", required=True, help="Path to config.yaml")
    train_parser.add_argument("--stopwords", action="store_true", help="Remove stopwords during keyword enrichment")

    # 🧪 New: Export appended_dict for validation
    export_parser = subparsers.add_parser("export_dict", help="Run pipeline up to appended_dict and export as pickles")
    export_parser.add_argument("--config", required=True, help="Path to config.yaml")
    export_parser.add_argument("--export_dir", required=True, help="Directory to export pickled DataFrames")

    args = parser.parse_args()
    print(f" Parsed command: {args.command}")

    if args.command == "preprocess":
        preprocess_dict, preprocess_metadata = preprocess(config_path=args.config, output_path=args.output, summary_path=args.summary_json)

    elif args.command == "metrics":
        metrics_df, metrics_metadata = metrics(config_path=args.config, output_path=args.output, summary_path=args.summary_json)

    elif args.command == "keywords":
        keywords_df, keywords_metadata = keywords(args.config, args.output, summary_path=args.summary_json, remove_stopwords=args.stopwords)
    
    elif args.command == "train":
        finalize_training_dataset(args.config)

    elif args.command == "export_dict":
        print(" Entering export_dict block")
        from scripts.config_loader import load_config
        from scripts.logger import configure_logger
        from scripts.loader import load_dataframes
        from scripts.preprocessing import rename_dataframe_columns
        from scripts.merge import append_dataframes_by_folder

        import os
        import pandas as pd

        config = load_config(args.config)
        logger = configure_logger(config=config)

        raw_dict = load_dataframes(args.config, logger)
        rename_dict = rename_dataframe_columns(config, raw_dict, logger)
        appended_dict = append_dataframes_by_folder(config, rename_dict, logger)

        os.makedirs(args.export_dir, exist_ok=True)

        print(" Exporting the following keys:", list(appended_dict.keys()))  # Sanity print
        for name, df in appended_dict.items():
            path = os.path.join(args.export_dir, f"{name}.pkl")
            df.to_pickle(path)
            print(f" Saved {name}.pkl to {path}")

if __name__ == "__main__":
    main()
