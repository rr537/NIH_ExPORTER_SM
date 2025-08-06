import pandas as pd
from typing import Optional, Dict, Any

def assemble_mlexport_metadata(
    mlexport_df: pd.DataFrame,
    dropped_df: Optional[pd.DataFrame],
    mlexport_summary: Optional[Dict[str, Any]],
    config: Dict
) -> Dict[str, Any]:
    """
    Assembles metadata dictionary after filtering process.

    Args:
        mlexport_df: Finalized DataFrame.
        dropped_df: Optional DataFrame of dropped rows.
        mlexport_summary: Optional ML-specific summary dict.
        config: config dict

    Returns:
        Dictionary with filtering metadata.
    """
    # Fetch config-based flag
    export_dropped = config.get("export_drop_output", False)

    return {
        "mlexport_summary": mlexport_summary or {},
        "total_rows": int(mlexport_df.shape[0]),
        "total_columns": int(mlexport_df.shape[1]),
        "exported_dropped_rows": int(dropped_df.shape[0]) if export_dropped and dropped_df is not None else 0
    }

def build_mlexport_summary(
    mlexport_stats: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Builds a structured summary from mlexport_stats for ML training output.

    Args:
        mlexport_stats: Dictionary containing mlexport metadata.

    Returns:
        Nested summary dictionary.
    """
    summary = {}

    if mlexport_stats:
        fs = mlexport_stats.get("mlexport_summary", {})
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
                "total_rows": mlexport_stats.get("total_rows"),
                "total_columns": mlexport_stats.get("total_columns"),
                "exported_dropped_rows": mlexport_stats.get("exported_dropped_rows", 0)
            }
        }

    return summary
