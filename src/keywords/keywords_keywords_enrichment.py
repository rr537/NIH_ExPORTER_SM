import pandas as pd
import numpy as np
from typing import List, Tuple, Dict, Optional
from flashtext import KeywordProcessor
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import logging

def enrich_with_keyword_metrics(
    df: pd.DataFrame,
    config: Dict,
    treatments: List[str],
    diseases: List[str],
    logger: Optional[logging.Logger] = None
) -> Tuple[pd.DataFrame, Dict]:
    """
    Enrichs DataFrame with keyword counts and matched treatment/disease terms.

    Adds:
        - total count
        - total unique count
        - flagged (matched terms)

    Returns:
        Tuple of enriched DataFrame and enrichment_summary
    """
    text_cols = config.get("text_columns", [])
    available_cols = [col for col in text_cols if col in df.columns]

    if not available_cols:
        if logger:
            logger.warning("No valid text columns found in DataFrame for enrichment.")
        return df, {}

    # Combine text across columns
    df["combined_text"] = df[available_cols].apply(
        lambda row: " | ".join(str(x).lower() for x in row if pd.notnull(x)),
        axis=1
    )

    # FlashText setup
    keyword_processor = KeywordProcessor()
    keyword_processor.add_keywords_from_list([kw.lower() for kw in treatments + diseases])

    # Create batches
    def chunk_series(series: pd.Series, chunk_size: int) -> List[pd.Series]:
        return [series[i:i + chunk_size] for i in range(0, len(series), chunk_size)]

    max_workers = config.get("workers", 4)
    chunk_size = max(1, len(df) // max_workers)
    chunks = chunk_series(df["combined_text"], chunk_size)

    # Batch processing
    def process_batch(batch: pd.Series) -> Tuple[List[int], List[int], List[List[str]]]:
        total, unique, flagged = [], [], []
        for text in batch:
            keywords = keyword_processor.extract_keywords(text)
            total.append(len(keywords))
            unique.append(len(set(keywords)))
            flagged.append(list(dict.fromkeys(keywords)))
        return total, unique, flagged

    # Parallel execution
    with ThreadPoolExecutor(max_workers=max_workers, thread_name_prefix="flash") as executor:
        results = list(executor.map(process_batch, chunks))

    # Flatten results
    total_flat = [count for batch in results for count in batch[0]]
    unique_flat = [count for batch in results for count in batch[1]]
    flagged_flat = [kw_list for batch in results for kw_list in batch[2]]

    # Assign enrichment columns
    df["total count"] = pd.Series(total_flat, index=df.index)
    df["total unique count"] = pd.Series(unique_flat, index=df.index)
    df["flagged"] = pd.Series(flagged_flat, index=df.index)
    df.drop(columns="combined_text", inplace=True)

    # Build summary
    enrichment_summary = {
        "total_rows_processed": len(df),
        "text_columns_used": available_cols,
        "max_workers": max_workers,
        "chunk_size": chunk_size,
        "keyword_pool_size": len(set(treatments + diseases)),
        "treatment_pool_size": len(set(treatments)),
        "disease_pool_size": len(set(diseases)),
        "total_keyword_hits": sum(total_flat),
        "avg_hits_per_row": round(np.mean(total_flat), 2),
        "avg_unique_per_row": round(np.mean(unique_flat), 2),
        "rows_with_hits": sum(1 for c in total_flat if c > 0),
        "rows_with_hits_pct": round(sum(1 for c in total_flat if c > 0) / len(df) * 100, 2),
        "rows_without_hits": sum(1 for c in total_flat if c == 0),
        "rows_without_hits_pct": round(sum(1 for c in total_flat if c == 0) / len(df) * 100, 2),
        "rows_flagged": sum(1 for flagged in flagged_flat if flagged),
        "top_flagged_terms": Counter([kw for row in flagged_flat for kw in row]).most_common(10)
    }

    if logger:
        logger.info(f"Keyword enrichment complete — {df.shape[0]:,} rows processed")

    return df, enrichment_summary
