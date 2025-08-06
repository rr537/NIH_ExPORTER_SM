from common.config_loader import load_config
from common.logger import configure_logger
from common.path_utils import resolve_input_files, resolve_output_path
from common.io_utils import export_summary_json
from .metrics_merge import merge_linked_dataframes
from .metrics_aggregate import aggregate_project_outputs
from .metrics_dedupe import remove_true_duplicates_from_df
from .metrics_summary import assemble_metrics_metadata, build_metrics_summary
from .metrics_io import load_pickle_dataframes, export_metrics_csv
from typing import Optional, List 


def metrics(pickles: Optional[List[str]], config_path: str, output_path: str, summary_path: str):
    # 1. Load configuration and set up logger
    config = load_config(config_path)
    logger = configure_logger(config.get("loglevel", "INFO"))

     # 2. Resolve input pickle files
    pickle_map = resolve_input_files("metrics", pickles, config, logger)

     # 3. Load all DataFrames from pickle files into a dict
    appended_dict = load_pickle_dataframes (pickle_map, logger)
    
    # 4. Run metrics pipeline 
    linked_dict, linked_summary = merge_linked_dataframes(appended_dict, logger) # Merge linked records
    aggregate_df, aggregate_summary = aggregate_project_outputs(linked_dict, appended_dict, logger) # Aggregate project outcomes
    metrics_df, dedupe_summary = remove_true_duplicates_from_df(aggregate_df, logger) # Deduplicate records

    # 5. Resolve output directory
    output_dir = resolve_output_path(stage ="metrics", output_path = output_path, config=config, logger=logger)

    # 6. Export metrics DataFrame to CSV
    export_metrics_csv (metrics_df, output_dir, logger)

    # 7. Prepare metadata
    metadata_raw = assemble_metrics_metadata(metrics_df, linked_summary, aggregate_summary, dedupe_summary)

    # 8. Build summary statistics
    metrics_summary = build_metrics_summary(metadata_raw)

    # 9. Export summary to JSON
    export_summary_json(metrics_summary, output_dir, default_filename="metrics_summary.json", summary_path=summary_path, logger=logger)

    return metrics_df, metrics_summary


