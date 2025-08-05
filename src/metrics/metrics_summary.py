import pandas as pd
from typing import Dict, Any

def assemble_metrics_metadata(
    metrics_df: pd.DataFrame,
    linked_summary: Dict[str, Any],
    aggregate_outcomes_summary: Dict[str, Any],
    dedupe_summary: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Assembles raw summary components from metrics pipeline into a unified metadata dictionary.
    """
    return {
        "linked_summary": linked_summary,
        "aggregate_outcomes_summary": aggregate_outcomes_summary,
        "dedupe_summary": dedupe_summary,
        "total_rows": int(metrics_df.shape[0]),
        "total_columns": int(metrics_df.shape[1])
    }

def build_metrics_summary(
    metrics_metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Builds structured summary statistics for metrics stage based on input metadata.
    """
    linked = metrics_metadata.get("linked_summary", {})
    aggregate = metrics_metadata.get("aggregate_outcomes_summary", {})
    dedupe = metrics_metadata.get("dedupe_summary", {})
    rows = metrics_metadata.get("total_rows", None)
    cols = metrics_metadata.get("total_columns", None)

    summary = {}

    if linked:
        summary["linked by shared identifier"] = {
            "by_'APPLICATION_ID'": {
                key: {
                    "source_shapes": {
                        "PRJ": value.get("PRJ", {}),
                        "PRJABS": value.get("PRJABS", {})
                    },
                    "merged_shape": value.get("merged", {}),
                    "change_from_merge": value.get("changes", {})
                }
                for key, value in linked.items()
            }
        }

    summary["aggregation"] = {
        "aggregate_outcomes": aggregate
    }

    summary["deduplication"] = {
        "dedupe_summary": dedupe
    }

    summary["dimensions_of_metrics_dataset"] = {
        "total_data_dimensions": {
            "rows": rows,
            "columns": cols
        }
    }

    return summary
