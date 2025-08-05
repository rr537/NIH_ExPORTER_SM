import pandas as pd
from typing import Optional, Dict, Any

def assemble_finalize_metadata(
    finalized_df: pd.DataFrame,
    dropped_df: Optional[pd.DataFrame] = None,
    finalize_summary: Optional[Dict[str, Any]] = None,
    drop_rows: bool = False
) -> Dict[str, Any]:
    """
    Assembles metadata dictionary after finalization process.

    Args:
        finalized_df: Finalized DataFrame.
        dropped_df: Optional DataFrame of dropped rows.
        finalize_summary: Optional ML-specific summary dict.
        drop_rows: Whether dropped rows were exported.

    Returns:
        Dictionary with finalization metadata.
    """
    return {
        "finalize_summary": finalize_summary or {},
        "total_rows": int(finalized_df.shape[0]),
        "total_columns": int(finalized_df.shape[1]),
        "exported_dropped_rows": int(dropped_df.shape[0]) if drop_rows and dropped_df is not None else 0
    }

def build_finalize_summary(
    finalize_stats: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Builds a structured summary from finalize_stats for ML training output.

    Args:
        finalize_stats: Dictionary containing finalize metadata.

    Returns:
        Nested summary dictionary.
    """
    summary = {}

    if finalize_stats:
        fs = finalize_stats.get("finalize_summary", {})
        summary["ml_training"] = {
            "filter_summary": {
                "ml_columns_used": fs.get("ml_columns_used", []),
                "cutoff_value": fs.get("cutoff_value"),
                "total_input_rows": fs.get("total_input_rows"),
                "total_retained_rows": fs.get("total_retained_rows"),
                "total_dropped_rows": fs.get("total_dropped_rows"),
                "percent_retained": fs.get("percent_retained"),
                "percent_dropped": fs.get("percent_dropped")
            },
            "output_dimensions": {
                "total_rows": finalize_stats.get("total_rows"),
                "total_columns": finalize_stats.get("total_columns"),
                "exported_dropped_rows": finalize_stats.get("exported_dropped_rows", 0)
            }
        }

    return summary
