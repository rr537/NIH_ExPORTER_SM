from typing import Optional, Dict, Any

def build_summary(
    preprocess_stats: Optional[Dict[str, Any]] = None,
    metrics_stats: Optional[Dict[str, Any]] = None,
    keywords_stats: Optional[Dict[str, Any]] = None,
    finalize_stats: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """
    Combine metadata from preprocessing and enrichment steps into a unified summary.
    Each input is optional and can be passed independently.
    """
    summary = {}

    # 🧪 Add preprocessing stats 
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

    # 🌿 Add metrics stats 
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

    # 🌿 Add keyword enrichment stats 
    if keywords_stats:
        summary["enrichment"] = {
            "keyword_library": {
                "treatment_terms": keywords_stats["keywords_summary"]["keyword_counts"]["treatment_terms"],
                "disease_terms": keywords_stats["keywords_summary"]["keyword_counts"]["disease_terms"],
                "total_terms": (
                    keywords_stats["keywords_summary"]["keyword_counts"]["treatment_terms"] +
                    keywords_stats["keywords_summary"]["keyword_counts"]["disease_terms"]
                )
            },
            "terms_used": keywords_stats["keywords_summary"]["keyword_lists"],
            "enrichment_metrics": {
                "total_rows_processed": keywords_stats["enrichment_summary"].get("total_rows_processed"),
                "text_columns_used": keywords_stats["enrichment_summary"].get("text_columns_used"),
                "keyword_pool_size": keywords_stats["enrichment_summary"].get("keyword_pool_size"),
                "treatment_pool_size": keywords_stats["enrichment_summary"].get("treatment_pool_size"),
                "disease_pool_size": keywords_stats["enrichment_summary"].get("disease_pool_size"),
                "total_keyword_hits": keywords_stats["enrichment_summary"].get("total_keyword_hits"),
                "avg_hits_per_row": keywords_stats["enrichment_summary"].get("avg_hits_per_row"),
                "avg_unique_per_row": keywords_stats["enrichment_summary"].get("avg_unique_per_row"),
                "rows_with_hits": keywords_stats["enrichment_summary"].get("rows_with_hits"),
                "rows_with_hits_pct": keywords_stats["enrichment_summary"].get("rows_with_hits_pct"),
                "rows_without_hits": keywords_stats["enrichment_summary"].get("rows_without_hits"),
                "rows_without_hits_pct": keywords_stats["enrichment_summary"].get("rows_without_hits_pct"),
                "rows_flagged": keywords_stats["enrichment_summary"].get("rows_flagged"),
                "top_flagged_terms": keywords_stats["enrichment_summary"].get("top_flagged_terms", [])
            },
            "output_dimensions": {
                "rows": keywords_stats.get("total_rows"),
                "columns": keywords_stats.get("total_columns")
            }
        }
    # 🌿 Add finalize dataset stats
    if finalize_stats:
        summary["ml_training"] = {
            "filter_summary": {
                "ml_columns_used": finalize_stats["finalize_summary"].get("ml_columns_used", []),
                "total_input_rows": finalize_stats["finalize_summary"].get("total_input_rows"),
                "total_retained_rows": finalize_stats["finalize_summary"].get("total_retained_rows"),
                "total_dropped_rows": finalize_stats["finalize_summary"].get("total_dropped_rows"),
                "percent_retained": finalize_stats["finalize_summary"].get("percent_retained"),
                "percent_dropped": finalize_stats["finalize_summary"].get("percent_dropped"),
                "retained_index_range": finalize_stats["finalize_summary"].get("retained_index_range"),
                "dropped_index_range": finalize_stats["finalize_summary"].get("dropped_index_range"),
                "summary_type": finalize_stats["finalize_summary"].get("summary_type", "ML Training Filter")
            },
            "output_dimensions": {
                "total_rows": finalize_stats.get("total_rows"),
                "total_columns": finalize_stats.get("total_columns"),
                "dropped_rows": finalize_stats.get("dropped_rows", 0)
            }
        }

    return summary
