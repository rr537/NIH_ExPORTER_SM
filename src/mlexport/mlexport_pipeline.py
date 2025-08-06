from common.config_loader import load_config
from common.logger import configure_logger
from common.path_utils import resolve_input_files, resolve_output_path
from common.io_utils import export_summary_json
from .mlexport_io import load_keywords_dataframe, export_mlexport_csv
from .mlexport_transform import filter_df
from .mlexport_summary import assemble_mlexport_metadata, build_mlexport_summary


def mlexport(keywords: str, config_path: str, output_path: str, summary_path: str):
    # 1️. Load config and initialize logger
    config = load_config(config_path) 
    logger = configure_logger(config=config)

    # 2️. Resolve input path and ingest keywords DataFrame
    keywords_path = resolve_input_files("mlexport", keywords, config, logger)
    keywords_df = load_keywords_dataframe(keywords_path, logger)

    # 3️. Run filtering logic
    MLdf, MLdf_dropped, mlexport_summary = filter_df(
        keywords_df,
        config=config,
        logger=logger
    )

     # 4. Resolve output directory
    output_dir = resolve_output_path("mlexport", output_path, config, logger)

    # 5. Export MLexport CSV
    export_mlexport_csv(MLdf, MLdf_dropped, output_dir, config, logger)

    # 6. Build metadata and summary
    mlexport_metadata = assemble_mlexport_metadata(MLdf, MLdf_dropped, mlexport_summary, config)

    # 7. Build summary statistics
    mlexport_summary = build_mlexport_summary(mlexport_metadata)

    # 8. Export summary JSON
    export_summary_json(mlexport_summary, output_dir, default_filename="mlexport_summary.json", summary_path=summary_path, logger=logger)

    return MLdf, mlexport_summary
