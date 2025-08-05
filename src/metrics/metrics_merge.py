import pandas as pd
import logging
from typing import Dict, Tuple, Optional 

def merge_linked_dataframes(
    dataframes: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None,
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, dict]]:
    """
    Merges semantically linked data sources:
    - PRJ + PRJABS → on 'APPLICATION_ID'

    Args:
        dataframes: Dict of cleaned folder → appended DataFrame
        logger: Optional logger

    Returns:
        Tuple containing:
        - Dict of merged DataFrames with composite key names
        - Dict of summary metrics detailing merge effects
    """
    # Grab sources
    prj = dataframes.get("PRJ")
    prjabs = dataframes.get("PRJABS")

    # Validate missing inputs
    missing_sources = []
    if prj is None: missing_sources.append("PRJ")
    if prjabs is None: missing_sources.append("PRJABS")

    if missing_sources and logger:
        logger.warning(f"Missing required merge sources: {missing_sources}")

    linked_dict = {}
    linked_summary_dict = {}

    # Merge PRJ + PRJABS
    if prj is not None and prjabs is not None:
        try:
            # Ensure APPLICATION_ID is string and stripped for consistency
            prj["APPLICATION_ID"] = prj["APPLICATION_ID"].astype(str).str.strip().str.upper()
            prjabs["APPLICATION_ID"] = prjabs["APPLICATION_ID"].astype(str).str.strip().str.upper()

            # Record pre-merge dimensions
            prj_shape = prj.shape
            prjabs_shape = prjabs.shape

            # Perform merge
            joined = pd.merge(prj, prjabs, on="APPLICATION_ID", how="left")

            # Record post-merge dimensions
            joined_shape = joined.shape
            rows_added = joined_shape[0] - prj_shape[0]
            cols_added = joined_shape[1] - prj_shape[1]

            # Fill summary
            linked_summary_dict["PRJ_PRJABS"] = {
                "PRJ": {"rows": prj_shape[0], "cols": prj_shape[1]},
                "PRJABS": {"rows": prjabs_shape[0], "cols": prjabs_shape[1]},
                "merged": {"rows": joined_shape[0], "cols": joined_shape[1]},
                "changes": {"rows_added": rows_added, "cols_added": cols_added},
            }

            linked_dict["PRJ_PRJABS"] = joined
            if logger:
                logger.info(f"Merged PRJ + PRJABS: {joined_shape[0]:,} rows × {joined_shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error("Failed merging PRJ and PRJABS", exc_info=True)

    return linked_dict, linked_summary_dict