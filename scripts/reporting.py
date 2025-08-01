from typing import Optional, Dict, Any

def build_summary(
    preprocess_stats: Optional[Dict[str, Any]] = None,
    metrics_stats: Optional[Dict[str, Any]] = None,
    enrich_stats: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Combine metadata from preprocessing and enrichment steps into a unified summary.
    Each input is optional and can be passed independently.
    """
    summary = {}

    # 🧪 Add preprocessing stats if available
    if preprocess_stats:
        summary["initial_load"] = {
            "initial_folder_stats": [
                {
                    "folder": fs.get("folder"),
                    "file_count": fs.get("file_count"),
                    "total_raw_rows": fs.get("total_raw_rows"),
                    "total_memory": fs.get("total_memory"),
                }
                for fs in preprocess_stats.get("load_summary", [])
            ]
        }

        summary["preprocessing"] = {
            "columns_renamed": preprocess_stats.get("rename_log", [])
        }

        summary["appended"] = {
            "folder_summaries": [
                {
                    "total_rows_after_append": fs.get("appended_rows", None),
                    "total_columns_after_append": fs.get("appended_columns", None),
                    "new_columns_added": fs.get("new_columns_added", 0),
                    "unexpected_columns": fs.get("unexpected_columns", []),
                    "skipped_due_to_mismatch": fs.get("skipped_due_to_mismatch", False),
                    "append_error": fs.get("append_error", None),
                }
                for fs in preprocess_stats.get("load_summary", [])
            ],
            "total_rows": preprocess_stats.get("total_rows", None),
            "total_columns": preprocess_stats.get("total_columns", None)
        }

    # 🌿 Add metrics stats if available
    if metrics_stats:
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
                for key, value in metrics_stats.get("linked_summary", {}).items()
            }
        }

        summary["aggregation"] = {
            "aggregate_outcomes": metrics_stats.get("aggregate_outcomes_summary", {})
        }

        summary["deduplication"] = {
            "dedupe_summary": metrics_stats.get("dedupe_summary", {})
        }

        summary["dimensions_of_metrics_dataset"] = {
            "total_data_dimensions": {
                "rows": metrics_stats.get("total_rows", None),
                "columns": metrics_stats.get("total_columns", None)
            }
        }

    # 🌿 Add enrichment stats if available
    if enrich_stats:
        summary["enrichment"] = {
            "features_added": enrich_stats.get("features_added", []),
            "rows_retained": enrich_stats.get("rows_retained", None),
            "data_quality_flags": enrich_stats.get("quality_flags", {})
        }

    return summary
