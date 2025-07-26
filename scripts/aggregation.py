import pandas as pd
import logging
from typing import Dict, Optional

def normalize_columns(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """
    Strips whitespace and uppercases values in specified columns to normalize formatting.
    """
    for col in cols:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.upper()
    return df

def count_unique_pairs(
    df: pd.DataFrame,
    combo_cols: list[str],
    count_name: str,
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    
    try:
        duplicates = df.duplicated(subset=combo_cols, keep=False)
        if logger:
            logger.info(f" Found {duplicates.sum():,} duplicated combos in {count_name}.")
        unique = df.drop_duplicates(subset=combo_cols)
        return (
            unique.groupby("PROJECT_NUMBER")
            .size()
            .reset_index(name=count_name)
        )
    except Exception as e:
        if logger:
            logger.error(f" Failed to process {count_name}", exc_info=True)
        return pd.DataFrame(columns=["PROJECT_NUMBER", count_name])


def aggregate_project_outputs(
    linked_merged: Dict[str, pd.DataFrame],
    merged_dataframes: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Aggregates publication, patent, and clinical study counts by PROJECT_NUMBER
    into a unified project-level DataFrame.
    """
    prj_df = linked_merged["PRJ_PRJABS"].copy()
    final_df = prj_df

# 📚 Publication count via PROJECT_NUMBER + PMID combination
    publications = merged_dataframes.get("PUBLINK")
    if publications is not None:
        try:
            # Step 1: Normalize columns for consistency
            publications = normalize_columns(publications, ["PMID", "PROJECT_NUMBER"])

            # Step 2: Count unique PROJECT_NUMBER + PMID pairs
            pub_count = count_unique_pairs(publications, ["PROJECT_NUMBER", "PMID"], "publication count", logger)

            # Step 3: Merge into final project-level dataframe
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
            
            #Step 1: Normalize columns for consistency
            patents = normalize_columns(patents, ["PROJECT_NUMBER", "PATENT_ID"])
            
            # Step 2: Count unique PROJECT_NUMBER + PATENT_ID pairs
            patent_count = count_unique_pairs(patents, ["PROJECT_NUMBER", "PATENT_ID"], "patent count", logger)

            # Step 3: Merge into final_df
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
            # Step 1: Normalize columns for consistency
            studies = normalize_columns(studies, ["PROJECT_NUMBER", "CLINICAL_TRIAL_ID"])
            
            # Step 2: Count unique PROJECT_NUMBER + ClinicalTrials.gov ID pairs
            study_count = count_unique_pairs(studies, ["PROJECT_NUMBER", "CLINICAL_TRIAL_ID"], "clinical study count", logger)
            
            # Step 3: Merge into final_df
            final_df = final_df.merge(study_count, on="PROJECT_NUMBER", how="left")
            final_df["clinical study count"] = final_df["clinical study count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique clinical study count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error(" Failed to compute or merge clinical study counts", exc_info=True)

    return final_df
