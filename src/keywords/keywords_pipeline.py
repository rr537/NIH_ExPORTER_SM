from common.config_loader import load_config
from common.logger import configure_logger
from common.path_utils import resolve_output_path, resolve_input_files
from .keywords_io import load_metrics_dataframe, export_keywords_csv, export_summary_json
from .keywords_keywords_generator import prepare_keywords
from .keywords_keywords_enrichment import enrich_with_keyword_metrics
from .keywords_summary import assemble_keywords_metadata, build_keywords_summary

def keywords(metrics: str, config_path: str, output_path: str, summary_path: str, remove_stopwords: bool = False):
    
    # Load configuration and logger
    config = load_config(config_path)
    logger = configure_logger(config.get("loglevel", "INFO"))

    # Resolve input path and load metrics DataFrame
    input_path = resolve_input_files("keywords", metrics, config, logger)
    metrics_df = load_metrics_dataframe(input_path, logger)

    # Run keyword preparation and enrichment
    treatments, diseases, keywords_summary = prepare_keywords(config, logger, remove_stopwords=remove_stopwords)
    keywords_df, enrichment_summary = enrich_with_keyword_metrics(metrics_df, config, treatments, diseases, logger)

    # Resolve output directory
    output_dir = resolve_output_path("keywords", output_path, config, logger)

    # Export keyword-enriched CSV
    export_keywords_csv(keywords_df, output_dir, logger)

    # Prepare metadata
    metadata_raw = assemble_keywords_metadata(keywords_df, keywords_summary, enrichment_summary)
    keywords_summary = build_keywords_summary(metadata_raw)

    export_summary_json(keywords_summary, output_dir, summary_path, logger)

    return keywords_df, keywords_summary
