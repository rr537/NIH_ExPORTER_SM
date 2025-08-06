from pathlib import Path
from typing import Any, Dict, Optional, Union
import json
import logging


def export_summary_json(
    summary: Dict[str, Any],
    output_dir: Union[str, Path],
    default_filename: str = "summary.json",
    summary_path: Optional[Union[str, Path]] = None,
    logger: Optional[logging.Logger] = None
) -> Path:
    """
    Exports a summary dictionary to a JSON file.

    Args:
        summary: Dictionary to export.
        output_dir: Base directory for saving the file.
        default_filename: Default filename if summary_path is not provided.
        summary_path: Optional full path override.
        logger: Optional logger for info/error messages.

    Returns:
        Path to the saved summary file.
    """
    final_path = Path(summary_path).resolve() if summary_path else output_dir / default_filename

    final_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(final_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        if logger:
            logger.info(f"Summary JSON exported to: {final_path}")
    except Exception as e:
        if logger:
            logger.error(f"Failed to export summary JSON: {str(e)}", exc_info=True)

    return final_path
