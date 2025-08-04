import pandas as pd
from typing import Dict, List, Any

def assemble_preprocessing_metadata(
    preprocess_dict: Dict[str, pd.DataFrame],
    load_summary: List[Dict[str, Any]],
    rename_summary: Dict[str, List[str]],
    appended_summary: Dict[str, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Combines input summaries and row/column counts into a unified preprocessing metadata dictionary.
    """
    combined_df = pd.concat(list(preprocess_dict.values()), ignore_index=True)

    metadata = {
        "load_summary": [
            {
                "folder": folder,
                "file_count": stats.get("file_count"),
                "total_raw_rows": stats.get("total_rows"),
                "total_memory": f"{stats.get('total_memory', 0):.2f} MB",
                **({
                    "appended_rows": appended_summary[folder].get("total_rows", 0),
                    "appended_columns": appended_summary[folder].get("total_columns", 0),
                    "unexpected_columns_added": appended_summary[folder].get("unexpected_columns_added", 0),
                    "unexpected_columns": appended_summary[folder].get("unexpected_columns", []),
                    "skipped_due_to_mismatch": appended_summary[folder].get("skipped", False),
                    "append_error": appended_summary[folder].get("error")
                } if folder in appended_summary else {})
            }
            for folder, stats in load_summary.items()
        ],
        "rename_summary": rename_summary,
        "total_rows": combined_df.shape[0],
        "total_columns": combined_df.shape[1]
    }

    return metadata

def build_preprocessing_summary(
    preprocess_metadata: Dict[str, Any]
) -> Dict[str, Any]:
    """
    Constructs a preprocessing-specific summary from metadata:
    Includes initial load stats, renaming records, folder-level append outcomes, and global dimensions.
    """
    folder_stats = preprocess_metadata.get("load_summary", [])

    summary = {
        "initial_load": {
            "initial_folder_stats": [
                {
                    "folder": fs.get("folder"),
                    "file_count": fs.get("file_count"),
                    "total_raw_rows": fs.get("total_raw_rows"),
                    "total_memory": fs.get("total_memory")
                }
                for fs in folder_stats
            ]
        },
        "preprocessing": {
            "columns_renamed": preprocess_metadata.get("rename_summary", {})
        },
        "appended": {
            "folder_summaries": [
                {
                    "folder": fs.get("folder"),
                    "appended_rows": fs.get("appended_rows"),
                    "appended_columns": fs.get("appended_columns"),
                    "unexpected_columns_added": fs.get("unexpected_columns_added"),
                    "unexpected_columns": fs.get("unexpected_columns"),
                    "skipped_due_to_mismatch": fs.get("skipped_due_to_mismatch"),
                    "append_error": fs.get("append_error")
                }
                for fs in folder_stats
            ],
            "total_rows": preprocess_metadata.get("total_rows"),
            "total_columns": preprocess_metadata.get("total_columns")
        }
    }

    return summary