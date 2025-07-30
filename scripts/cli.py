import sys
import os
from pathlib import Path
import json
# Add project root to Python path
sys.path.append(str(Path(__file__).resolve().parent.parent))

print("sys.path:")
for p in sys.path:
    print(" ", p)

import argparse
import pandas as pd
from typing import Dict, Tuple, List, Optional, Any  
from scripts.config_loader import load_config
from scripts.logger import configure_logger
from scripts.loader import load_dataframes
from scripts.pipeline import preprocess_pipeline, full_enrichment_pipeline, full_ml_pipeline, finalize_training_dataset
from scripts.keywords import load_keywords
from scripts.enrichment import enrich_with_keyword_metrics
from scripts.loader import (validate_data_sources, validate_folder_path)


def preprocess(config_path: str, output_path: str, summary_path: str = None)-> Tuple[Dict[str, pd.DataFrame], List[str], Dict[str, Any]]:
    # 1. Load configuration and set up logger
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    # 2. Resolve output directory
    output_dir = Path(output_path if output_path else config.get("output_dir", "results")).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    # 3. Validate config paths and data sources
    validate_folder_path(config_path, logger)
    validate_data_sources(config_path, logger)

    # 4. Run preprocessing pipeline
    appended_dict, rename_log, load_summary = preprocess_pipeline(config=config, logger=logger)

    # 5. Export each appended DataFrame to pickle
    print(" Exporting the following keys:", list(appended_dict.keys()))
    for name, df in appended_dict.items():
        path = output_dir / f"{name}.pkl"
        df.to_pickle(path)
        print(f" Saved {name}.pkl to {path}")

    # 6. Combine data for summary statistics
    appended_df_combined = pd.concat(list(appended_dict.values()), ignore_index=True)

    # 7. Build and export preprocessing summary
    
    # If no summary path provided, use default
    if summary_path is None:
        summary_path = output_dir / "preprocessing_summary.json"
    else:
        summary_path = Path(summary_path).resolve()

    summary_path.parent.mkdir(parents=True, exist_ok=True)

    # Build summary dictionary
    summary = {
        "initial_load": {"folder_stats": []},
        "preprocessing": {"columns_renamed": rename_log},
        "appended": {
            "total_rows": int(appended_df_combined.shape[0]),
            "total_columns": int(appended_df_combined.shape[1]),
        }
    }
    # Add initial load stats
    for folder, stats in load_summary.items():
        summary["initial_load"]["folder_stats"].append({
            "folder": folder,
            "file_count": stats["file_count"],
            "total_raw_rows": stats["total_rows"],
            "total_memory": f"{stats['total_memory']:.2f} MB"
        })
    # Add appended DataFrame stats
    with open(summary_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    logger.info(f" Preprocessing summary exported to: {summary_path}")

    return appended_dict, rename_log, load_summary


def enrich(config_path: str, output_path: str, remove_stopwords: bool = False):
    config = load_config(config_path)
    logger = configure_logger(config=config, loglevel=config.get("loglevel", "INFO"))

    validate_folder_path(config_path, logger)
    validate_data_sources(config_path, logger)

    enriched_df = full_enrichment_pipeline(
        config=config,
        logger=logger, 
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
    preprocess_parser.add_argument("--summary-json", help="Optional path to export preprocessing summary as JSON", required=False)

    # ✨ Enrichment step
    enrich_parser = subparsers.add_parser("enrich", help="Run full ML enrichment pipeline")
    enrich_parser.add_argument("--config", required=True, help="Path to config.yaml")
    enrich_parser.add_argument("--output", required=True, help="Path to output CSV")
    enrich_parser.add_argument("--stopwords", action="store_true", help="Remove English stopwords during keyword enrichment")

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
        preprocess(args.config, args.output, args.summary_json)

    elif args.command == "enrich":
        enrich(args.config, args.output, remove_stopwords=args.stopwords)
    
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
