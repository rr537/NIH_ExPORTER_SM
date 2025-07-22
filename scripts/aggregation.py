import pandas as pd
import logging
from typing import Dict, Optional

def aggregate_project_outputs(
    linked_merged: Dict[str, pd.DataFrame],
    merged_dataframes: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Aggregates publication, patent, and clinical study counts by PROJECT_NUMBER
    into a unified project-level DataFrame.
    """

    required_keys = ["PRJ_PRJABS", "PUB_PUBLINK"]
    missing = [key for key in required_keys if key not in linked_merged or linked_merged[key] is None]

    if missing:
        if logger:
            logger.warning(f" Missing required data: {missing}. Returning empty DataFrame.")
        return pd.DataFrame()

    prj_df = linked_merged["PRJ_PRJABS"].copy()
    pub_df = linked_merged["PUB_PUBLINK"]
    final_df = prj_df

    # 📚 Publication count
    try:
        pub_count = pub_df.groupby("PROJECT_NUMBER").size().reset_index(name="publication count")
        final_df = final_df.merge(pub_count, on="PROJECT_NUMBER", how="left")
        final_df["publication count"] = final_df["publication count"].fillna(0).astype(int)
        if logger:
            logger.info(f" Merged publication count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")
    except Exception as e:
        if logger:
            logger.error(" Failed to merge publication counts", exc_info=True)

    # 🧬 Patent count
    patents = merged_dataframes.get("Patents")
    if patents is not None:
        try:
            patent_count = patents.groupby("PROJECT_NUMBER").size().reset_index(name="patent count")
            final_df = final_df.merge(patent_count, on="PROJECT_NUMBER", how="left")
            final_df["patent count"] = final_df["patent count"].fillna(0).astype(int)
            if logger:
                logger.info(f" Merged patent count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")
        except Exception as e:
            if logger:
                logger.error(" Failed to merge patent counts", exc_info=True)

    # 🧪 Clinical study count
    studies = merged_dataframes.get("ClinicalStudies")
    if studies is not None:
        try:
            study_count = studies.groupby("PROJECT_NUMBER").size().reset_index(name="clinical study count")
            final_df = final_df.merge(study_count, on="PROJECT_NUMBER", how="left")
            final_df["clinical study count"] = final_df["clinical study count"].fillna(0).astype(int)
            if logger:
                logger.info(f" Merged clinical study count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")
        except Exception as e:
            if logger:
                logger.error(" Failed to merge clinical study counts", exc_info=True)

    return final_df
