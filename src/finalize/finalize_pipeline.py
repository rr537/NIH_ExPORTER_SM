from pathlib import Path
from common.config_loader import load_config
from common.logger import configure_logger
from common.path_utils import resolve_input_files, resolve_output_path
from finalize.finalize_io import load_keywords_dataframe, export_finalized_csv, export_summary_json
from finalize.finalize_transform import filter_df
from finalize.finalize_summary import assemble_finalize_metadata, build_finalize_summary


def finalize(keywords: str, config_path: str, output_path: str, summary_path: str, cutoff_value: int, drop_rows: bool = False):
    # 1️. Load config and initialize logger
    config = load_config(config_path)
    logger = configure_logger(config.get("loglevel", "INFO"))

    # 2️. Resolve input path and ingest keywords DataFrame
    keywords_path = resolve_input_files("finalize", keywords, config, logger)
    keywords_df = load_keywords_dataframe(keywords_path, logger)

    # 3️. Run finalization logic
    MLdf, MLdf_dropped, finalize_summary = filter_df(
        keywords_df,
        config=config,
        logger=logger,
        cutoff_value=cutoff_value
    )

     # 4. Resolve output directory
    output_dir = resolve_output_path("finalize", output_path, config, logger)

    # 5. Export finalized CSV
    export_finalized_csv(MLdf, MLdf_dropped, output_dir, logger, drop_rows)

    # 6. Build metadata and summary
    finalize_metadata = assemble_finalize_metadata(MLdf, MLdf_dropped, finalize_summary, drop_rows)

    # 7. Build summary statistics
    finalize_summary = build_finalize_summary(finalize_metadata)

    # 8. Export summary JSON
    export_summary_json(finalize_summary, output_dir, summary_path, logger)

    return MLdf, finalize_summary
