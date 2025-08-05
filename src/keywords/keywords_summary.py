from typing import Dict, List, Tuple
import pandas as pd

def assemble_keywords_metadata(
    keywords_df: pd.DataFrame,
    keywords_summary: Dict,
    enrichment_summary: Dict
) -> Dict:
    """
    Combines metadata for keyword enrichment process.

    Args:
        keywords_df: Enriched DataFrame containing keyword columns.
        keywords_summary: Summary dict with keyword counts and lists.
        enrichment_summary: Dict of enrichment metrics from processing.

    Returns:
        Combined metadata dictionary for downstream use.
    """
    return {
        "keywords_summary": keywords_summary,
        "enrichment_summary": enrichment_summary,
        "total_rows": int(keywords_df.shape[0]),
        "total_columns": int(keywords_df.shape[1])
    }

def build_keywords_summary(keywords_stats: Dict) -> Dict:
    """
    Constructs a structured summary from keywords metadata dictionary.

    Args:
        keywords_stats: Output from assemble_keywords_metadata()

    Returns:
        Nested summary dictionary for reporting/export
    """
    if not keywords_stats:
        return {}

    summary = {}

    # Keyword library statistics
    ks = keywords_stats["keywords_summary"]
    es = keywords_stats["enrichment_summary"]

    summary["enrichment"] = {
        "keyword_library": {
            "treatment_terms": ks["keyword_counts"]["treatment_terms"],
            "disease_terms": ks["keyword_counts"]["disease_terms"],
            "total_terms": ks["keyword_counts"]["treatment_terms"] + ks["keyword_counts"]["disease_terms"]
        },
        "terms_used": ks["keyword_lists"],
        "enrichment_metrics": {
            "total_rows_processed": es.get("total_rows_processed"),
            "text_columns_used": es.get("text_columns_used"),
            "keyword_pool_size": es.get("keyword_pool_size"),
            "treatment_pool_size": es.get("treatment_pool_size"),
            "disease_pool_size": es.get("disease_pool_size"),
            "total_keyword_hits": es.get("total_keyword_hits"),
            "avg_hits_per_row": es.get("avg_hits_per_row"),
            "avg_unique_per_row": es.get("avg_unique_per_row"),
            "rows_with_hits": es.get("rows_with_hits"),
            "rows_with_hits_pct": es.get("rows_with_hits_pct"),
            "rows_without_hits": es.get("rows_without_hits"),
            "rows_without_hits_pct": es.get("rows_without_hits_pct"),
            "rows_flagged": es.get("rows_flagged"),
            "top_flagged_terms": es.get("top_flagged_terms", [])
        },
        "output_dimensions": {
            "rows": keywords_stats.get("total_rows"),
            "columns": keywords_stats.get("total_columns")
        }
    }

    return summary
