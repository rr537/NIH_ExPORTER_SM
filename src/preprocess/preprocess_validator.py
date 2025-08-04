import logging
from pathlib import Path
from common.config_loader import load_config

def validate_config_paths(config_path: str, logger: logging.Logger) -> None:
    """Check for suspicious or absolute paths in config folder field."""
    config = load_config(config_path)
    folder_raw = config.get("folder", "")
    folder_path = Path(folder_raw)

    # Flag suspicious path segments
    if _is_suspicious_path(folder_raw):
        logger.warning(
            f"Suspicious folder path in config: '{folder_raw}' — "
            "consider using a relative path like 'data/raw'."
        )

    # Warn for absolute paths
    if folder_path.is_absolute():
        logger.warning(
            f"Absolute folder path detected: '{folder_path}' — "
            "relative paths help ensure portability."
        )

def validate_data_sources(config_path: str, logger: logging.Logger) -> None:
    """Confirm subfolders exist and contain CSV files."""
    config = load_config(config_path)
    project_root = Path(__file__).resolve().parents[2]
    data_root = project_root / config.get("folder", "")
    subfolders = config.get("subfolders", [])

    logger.info("Validating data sources...\n")

    for subfolder in subfolders:
        folder_path = data_root / subfolder
        if not folder_path.exists():
            logger.warning(f"Missing folder: {folder_path.resolve()}")
            continue

        csv_files = list(folder_path.glob("*.csv"))
        if not csv_files:
            logger.warning(f"Folder contains no CSVs: {folder_path.resolve()}")
        else:
            logger.info(f"Found {len(csv_files):,} CSV(s) in: {folder_path.resolve()}")

# Internal utilities
def _is_suspicious_path(path_str: str) -> bool:
    suspicious = ["..", "...", ".", "/", "\\"]
    return any(path_str.strip().startswith(s) for s in suspicious)
