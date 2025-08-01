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
        folder_stats = preprocess_stats.get("load_summary", [])

        summary["initial_load"] = {
            "initial_folder_stats": [
                {k: fs[k] for k in ("folder", "file_count", "total_raw_rows", "total_memory")}
                for fs in folder_stats
            ]
        }

        summary["preprocessing"] = {
            "columns_renamed": preprocess_stats.get("rename_summary", [])
        }

        summary["appended"] = {
            "folder_summaries": [
                {k: fs.get(k) for k in (
                    "folder",
                    "appended_rows",
                    "appended_columns",
                    "new_columns_added",
                    "unexpected_columns",
                    "skipped_due_to_mismatch",
                    "append_error"
                )}
                for fs in folder_stats
            ],
            "total_rows": preprocess_stats.get("total_rows"),
            "total_columns": preprocess_stats.get("total_columns")
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
