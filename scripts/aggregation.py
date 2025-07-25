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

# 📚 Publication count via PROJECT_NUMBER + PMID combination
    try:
        # Step 1: Concatenate PROJECT_NUMBER and PMID
        pub_df["project_pub_combo"] = pub_df["PROJECT_NUMBER"].astype(str) + "_" + pub_df["PMID"].astype(str)

        # Step 2: Detect and log duplicate combinations
        duplicate_combos = pub_df.duplicated(subset=["project_pub_combo"], keep=False)
        num_duplicates = duplicate_combos.sum()

        if logger:
            logger.info(f" Found {num_duplicates:,} duplicated PROJECT_NUMBER + PMID pairs in PUB_PUBLINK.")

        # Step 3: Count unique combinations per PROJECT_NUMBER
        unique_combos = pub_df[~duplicate_combos]  # or use .drop_duplicates(["project_pub_combo"])
        pub_count = (
            unique_combos.groupby("PROJECT_NUMBER")
            .size()
            .reset_index(name="publication count")
        )

        # Step 4: Merge into final project-level dataframe
        final_df = final_df.merge(pub_count, on="PROJECT_NUMBER", how="left")
        final_df["publication count"] = final_df["publication count"].fillna(0).astype(int)

        if logger:
            logger.info(f" Merged unique publication counts: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")

    except Exception as e:
        if logger:
            logger.error(" Failed to compute publication counts", exc_info=True)


# 🧬 Patent count using unique PROJECT_NUMBER + PATENT_ID pairs
    patents = merged_dataframes.get("Patents")
    if patents is not None:
        try:
            # Step 1: Concatenate PROJECT_NUMBER and PATENT_ID
            patents["project_patent_combo"] = patents["PROJECT_NUMBER"].astype(str) + "_" + patents["PATENT_ID"].astype(str)

            # Step 2: Identify duplicates
            duplicate_patent_combos = patents.duplicated(subset=["project_patent_combo"], keep=False)
            num_patent_duplicates = duplicate_patent_combos.sum()

            if logger:
                logger.info(f" Found {num_patent_duplicates:,} duplicated PROJECT_NUMBER + PATENT_ID combinations in Patents data.")

            # Step 3: Count unique combinations per PROJECT_NUMBER
            unique_patents = patents.drop_duplicates(subset=["project_patent_combo"])
            patent_count = (
                unique_patents.groupby("PROJECT_NUMBER")
                .size()
                .reset_index(name="patent count")
            )

            # Step 4: Merge into final_df
            final_df = final_df.merge(patent_count, on="PROJECT_NUMBER", how="left")
            final_df["patent count"] = final_df["patent count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique patent count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error(" Failed to compute or merge patent counts", exc_info=True)

# 🧪 Clinical study count using unique PROJECT_NUMBER + ClinicalTrials.gov ID pairs
    studies = merged_dataframes.get("ClinicalStudies")
    if studies is not None:
        try:
            # Step 1: Concatenate PROJECT_NUMBER and ClinicalTrials.gov ID
            studies["project_study_combo"] = studies["PROJECT_NUMBER"].astype(str) + "_" + studies["ClinicalTrials.gov ID"].astype(str)

            # Step 2: Identify duplicates
            duplicate_study_combos = studies.duplicated(subset=["project_study_combo"], keep=False)
            num_study_duplicates = duplicate_study_combos.sum()

            if logger:
                logger.info(f" Found {num_study_duplicates:,} duplicated PROJECT_NUMBER + ClinicalTrials.gov ID combinations in ClinicalStudies data.")

            # Step 3: Count unique combinations per PROJECT_NUMBER
            unique_studies = studies.drop_duplicates(subset=["project_study_combo"])
            study_count = (
                unique_studies.groupby("PROJECT_NUMBER")
                .size()
                .reset_index(name="clinical study count")
            )

            # Step 4: Merge into final_df
            final_df = final_df.merge(study_count, on="PROJECT_NUMBER", how="left")
            final_df["clinical study count"] = final_df["clinical study count"].fillna(0).astype(int)

            if logger:
                logger.info(f"✅ Merged unique clinical study count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error("❌ Failed to compute or merge clinical study counts", exc_info=True)

    return final_df
