from common.config_loader import load_config
from common.logger import configure_logger
from common.path_utils import resolve_output_path
from .preprocess_validator import validate_config_paths, validate_data_sources
from .preprocess_io import ingest_dataframes, save_pickle_files, export_summary_json
from .preprocess_transform import rename_columns, append_dataframes_by_folder
from .preprocess_summary import assemble_preprocessing_metadata, build_preprocessing_summary


def preprocess(config_path: str, output_path: str = None, summary_path: str = None):
    # 1. Load configuration and set up logger
    config = load_config(config_path)
    logger = configure_logger(config.get("loglevel", "INFO"))

    # 2. Validate config paths and data sources
    validate_config_paths(config_path, logger)
    validate_data_sources(config_path, logger)

    # 3. Run preprocessing pipeline
    raw_dict, load_summary = ingest_dataframes(config, logger)
    rename_dict, rename_summary = rename_columns(config, raw_dict, logger)
    appended_dict, appended_summary = append_dataframes_by_folder(config, rename_dict, logger)

    # 4. Resolve output directory
    output_dir = resolve_output_path(stage ="preprocess", output_path = output_path, config=config, logger=logger)

    # 5. Export each appended DataFrame to pickle
    save_pickle_files(appended_dict, output_dir, logger)

    # 6.  Prepare metadata
    metadata_raw = assemble_preprocessing_metadata(appended_dict, load_summary, rename_summary, appended_summary)
    
    # 7. Build summary statistics
    preprocess_summary = build_preprocessing_summary(metadata_raw)

    # 8. Export summary to JSON
    export_summary_json(preprocess_summary, output_dir, logger, summary_path)

    return appended_dict, preprocess_summary
