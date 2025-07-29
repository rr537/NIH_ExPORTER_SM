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
) -> tuple[pd.DataFrame, pd.DataFrame]:
    
    try:
        duplicates_all = df.duplicated(subset=combo_cols, keep=False) # Count of all duplicate rows
        duplicates_extra = df.duplicated(subset=combo_cols, keep='first') # Count of extra duplicates beyond the first occurrence
        distinct_duplicate_rows = df[duplicates_all].drop_duplicates(subset=combo_cols) # Keep only distinct duplicate rows

        dedup_summary = pd.DataFrame([{
            "category": count_name,
            "unique_duplicate_rows": distinct_duplicate_rows.shape[0],
            "total_duplicates": duplicates_all.sum(),
            "extra_duplicates": duplicates_extra.sum()
        }])

        if logger:
            logger.info(
                f" In {count_name}: {dedup_summary['unique_duplicate_rows'][0]:,} unique duplicates, "
                f"{dedup_summary['total_duplicates'][0]:,} total duplicates, "
                f"{dedup_summary['extra_duplicates'][0]:,} extra duplicates."
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
        return pd.DataFrame(columns=["PROJECT_NUMBER", count_name]), pd.DataFrame()



def aggregate_project_outputs(
    linked_merged: Dict[str, pd.DataFrame],
    appended_dict: Dict[str, pd.DataFrame],
    logger: Optional[logging.Logger] = None,
    outcomes_dedup_summary_path: str = "results/outcomes_dedup_summary.csv"
) -> pd.DataFrame:
    """
    Aggregates publication, patent, and clinical study counts by PROJECT_NUMBER
    into a unified project-level DataFrame.
    """
    prj_df = linked_merged["PRJ_PRJABS"].copy()
    final_df = prj_df
    final_df.rename(columns={"PROJECT_NUMBER_x": "PROJECT_NUMBER"}, inplace=True)

# 📚 Publication count via PROJECT_NUMBER + PMID combination
    publications = appended_dict.get("PUBLINK")
    if publications is not None:
        try:
            # Step 1: Normalize columns for consistency
            publications = normalize_columns(publications, ["PMID", "PROJECT_NUMBER"])

            # Step 2: Count unique PROJECT_NUMBER + PMID pairs
            pub_count, pub_summary = count_unique_pairs(publications, ["PROJECT_NUMBER", "PMID"], "publication count", logger)

            # Step 3: Merge into final project-level dataframe
            final_df = final_df.merge(pub_count, on="PROJECT_NUMBER", how="left")
            final_df["publication count"] = final_df["publication count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique publication counts: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")

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

            # Step 3: Merge into final_df
            final_df = final_df.merge(patent_count, on="PROJECT_NUMBER", how="left")
            final_df["patent count"] = final_df["patent count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique patent count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")

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
            
            # Step 3: Merge into final_df
            final_df = final_df.merge(study_count, on="PROJECT_NUMBER", how="left")
            final_df["clinical study count"] = final_df["clinical study count"].fillna(0).astype(int)

            if logger:
                logger.info(f" Merged unique clinical study count: {final_df.shape[0]:,} rows × {final_df.shape[1]:,} columns")

        except Exception as e:
            if logger:
                logger.error(" Failed to compute or merge clinical study counts", exc_info=True)

    # Accumulate all summaries
    summary_list = [pub_summary, patent_summary, study_summary]
    outcomes_dedup_summary= pd.concat(summary_list, ignore_index=True)

    return final_df, outcomes_dedup_summary
