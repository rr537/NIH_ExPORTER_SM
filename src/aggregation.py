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
) -> tuple[pd.DataFrame, dict]:

    try:
        duplicates_all = df.duplicated(subset=combo_cols, keep=False)
        duplicates_extra = df.duplicated(subset=combo_cols, keep='first')
        distinct_duplicate_rows = df[duplicates_all].drop_duplicates(subset=combo_cols)

        # Build dedup summary as a nested dict
        dedup_summary = {
            count_name: {
                "unique_duplicate_rows": distinct_duplicate_rows.shape[0],
                "total_duplicates": int(duplicates_all.sum()),
                "extra_duplicates": int(duplicates_extra.sum())
            }
        }

        if logger:
            row = dedup_summary[count_name]
            logger.info(
                f" In {count_name}: {row['unique_duplicate_rows']:,} unique duplicates, "
                f"{row['total_duplicates']:,} total duplicates, "
                f"{row['extra_duplicates']:,} extra duplicates."
            )

        unique = df.drop_duplicates(subset=combo_cols)
        count_df = (
            unique.groupby("PROJECT_NUMBER")
            .size()
            .reset_index(name=count_name)
        )
        return count_df, dedup_summary

    except Exception as e:
        if logger:
            logger.error(f" Failed to process {count_name}", exc_info=True)
        return pd.DataFrame(columns=["PROJECT_NUMBER", count_name]), {}

def aggregate_project_outputs(
    linked_merged: Dict[str, pd.DataFrame],
    appended_dict: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None
) -> pd.DataFrame:
    """
    Aggregates publication, patent, and clinical study counts by PROJECT_NUMBER
    into a unified project-level DataFrame.
    """
    prj_df = linked_merged["PRJ_PRJABS"].copy()
    aggregate_df = prj_df
    aggregate_df.rename(columns={"PROJECT_NUMBER_x": "PROJECT_NUMBER"}, inplace=True)

# 📚 Publication count via PROJECT_NUMBER + PMID combination
    publications = appended_dict.get("PUBLINK")
    if publications is not None:
        try:
            # Step 1: Normalize columns for consistency
            publications = normalize_columns(publications, ["PMID", "PROJECT_NUMBER"])

            # Step 2: Count unique PROJECT_NUMBER + PMID pairs
            pub_count, pub_summary = count_unique_pairs(publications, ["PROJECT_NUMBER", "PMID"], "publication count", logger)

            # Step 3: Merge into final project-level dataframe
            aggregate_df = aggregate_df.merge(pub_count, on="PROJECT_NUMBER", how="left")
            aggregate_df["publication count"] = aggregate_df["publication count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique publication counts: {aggregate_df.shape[0]:,} rows × {aggregate_df.shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error(" Failed to compute publication counts", exc_info=True)


# 🧬 Patent count using unique PROJECT_NUMBER + PATENT_ID pairs
    patents = appended_dict.get("Patents")
    if patents is not None:
        try:
            
            #Step 1: Normalize columns for consistency
            patents = normalize_columns(patents, ["PROJECT_NUMBER", "PATENT_ID"])
            
            # Step 2: Count unique PROJECT_NUMBER + PATENT_ID pairs
            patent_count, patent_summary = count_unique_pairs(patents, ["PROJECT_NUMBER", "PATENT_ID"], "patent count", logger)

            # Step 3: Merge into aggregate_df
            aggregate_df = aggregate_df.merge(patent_count, on="PROJECT_NUMBER", how="left")
            aggregate_df["patent count"] = aggregate_df["patent count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique patent count: {aggregate_df.shape[0]:,} rows × {aggregate_df.shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error(" Failed to compute or merge patent counts", exc_info=True)

# 🧪 Clinical study count using unique PROJECT_NUMBER + ClinicalTrials.gov ID pairs
    studies = appended_dict.get("ClinicalStudies")
    if studies is not None:
        try:
            # Step 1: Normalize columns for consistency
            studies = normalize_columns(studies, ["PROJECT_NUMBER", "ClinicalTrials.gov ID"])
            
            # Step 2: Count unique PROJECT_NUMBER + ClinicalTrials.gov ID pairs
            study_count, study_summary = count_unique_pairs(studies, ["PROJECT_NUMBER", "ClinicalTrials.gov ID"], "clinical study count", logger)
            
            # Step 3: Merge into aggregate_df
            aggregate_df = aggregate_df.merge(study_count, on="PROJECT_NUMBER", how="left")
            aggregate_df["clinical study count"] = aggregate_df["clinical study count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique clinical study count: {aggregate_df.shape[0]:,} rows × {aggregate_df.shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error(" Failed to compute or merge clinical study counts", exc_info=True)

    # Accumulate all summaries
    aggregate_outcomes_summary_dict = {
        **pub_summary,
        **patent_summary,
        **study_summary
    }

    return aggregate_df, aggregate_outcomes_summary_dict
