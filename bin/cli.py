import argparse
import sys
from pathlib import Path

# Add src as project root to Python path   
src_path = Path(__file__).resolve().parent.parent / "src"
sys.path.append(str(src_path))

from preprocess.preprocess_pipeline import preprocess
from metrics.metrics_pipeline import metrics
from keywords.keywords_pipeline import keywords
from finalize.finalize_pipeline import finalize 

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
    metrics_parser.add_argument("--pickles", default=None, help="Optional list of pickle file paths to process")
    metrics_parser.add_argument("--config", required=True, help="Path to config.yaml")
    metrics_parser.add_argument("--output", help="Path to output directory for aggregated outcomes dataset")
    metrics_parser.add_argument("--summary-json", help="Optional path to export metrics summary as JSON", required=False)

    # Keyword enrichment step
    keyword_parser = subparsers.add_parser("keywords", help="Count and flag keywords in project text related columns")
    keyword_parser.add_argument("--metrics", help="Optional path to metrics CSV file")
    keyword_parser.add_argument("--config", required=True, help="Path to config.yaml")
    keyword_parser.add_argument("--output", help="Path to output CSV")
    keyword_parser.add_argument("--summary-json", help="Optional path to export keyword summary as JSON", required=False)
    keyword_parser.add_argument("--stopwords", action="store_true", help="Remove English stopwords during keyword enrichment")

    # Finalize training dataset step
    finalize_parser = subparsers.add_parser("finalize", help="Filter out unnecessary columns and prepare final training dataset")
    finalize_parser.add_argument("--keywords", help="Optional path to keywords CSV file")
    finalize_parser.add_argument("--config", required=True, help="Path to config.yaml")
    finalize_parser.add_argument("--output", help="Optional Path to output CSV")
    finalize_parser.add_argument("--summary-json", help="Optional path to export training dataset summary as JSON", required=False)
    finalize_parser.add_argument("--cutoff_value", type=int, help="Cut off value for filtering rows based on keyword counts. Default = 1")
    finalize_parser.add_argument("--drop-output", action="store_true", help="Export dropped rows")

    args = parser.parse_args()
    print(f" Parsed command: {args.command}")

    if args.command == "preprocess":
        preprocess_dict, preprocess_metadata = preprocess(config_path=args.config, output_path=args.output, summary_path=args.summary_json)

    elif args.command == "metrics":
        metrics_df, metrics_metadata = metrics(pickles = args.pickles, config_path=args.config, output_path=args.output, summary_path=args.summary_json)

    elif args.command == "keywords":
        keywords_df, keywords_metadata = keywords(metrics = args.metrics, config_path=args.config, output_path=args.output, summary_path=args.summary_json, remove_stopwords=args.stopwords)
    
    elif args.command == "finalize":
        MLdf, finalize_metadata = finalize(keywords = args.keywords, config_path=args.config, output_path=args.output, summary_path=args.summary_json, cutoff_value = args.cutoff_value, drop_rows = args.drop_output)

if __name__ == "__main__":
    main()