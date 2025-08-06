from common.config_loader import load_config
from common.logger import configure_logger
from common.path_utils import resolve_output_path, resolve_input_files
from common.io_utils import export_summary_json
from .keywords_io import load_metrics_dataframe, export_keywords_csv
from .keywords_keywords_generator import prepare_keywords
from .keywords_keywords_enrichment import enrich_with_keyword_metrics
from .keywords_summary import assemble_keywords_metadata, build_keywords_summary

def keywords(metrics: str, config_path: str, output_path: str, summary_path: str):  
    # 1. Load configuration and logger
    config = load_config(config_path)
    logger = configure_logger(config=config)

    # 2. Resolve input path and load metrics DataFrame
    metrics_path = resolve_input_files("keywords", metrics, config, logger)
    metrics_df = load_metrics_dataframe(metrics_path, logger)

    # 3. Run keyword preparation and enrichment
    treatments, diseases, keywords_summary = prepare_keywords(config, logger)
    keywords_df, enrichment_summary = enrich_with_keyword_metrics(metrics_df, config, treatments, diseases, logger)

    # 4. Resolve output directory
    output_dir = resolve_output_path("keywords", output_path, config, logger)

    # 5. Export keyword-enriched CSV
    export_keywords_csv(keywords_df, output_dir, logger)

    # 6. Prepare metadata
    metadata_raw = assemble_keywords_metadata(keywords_df, keywords_summary, enrichment_summary)

    # 7. Build summary statistics
    keywords_summary = build_keywords_summary(metadata_raw)

    # 8. Export summary to JSON
    export_summary_json(keywords_summary, output_dir, default_filename="keywords_summary.json", summary_path=summary_path, logger=logger)

    return keywords_df, keywords_summary
