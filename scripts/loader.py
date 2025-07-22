import pandas as pd
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Dict, List, Optional, Tuple
import yaml

# 🧼 Individual file reader
def read_csv_file(file_path: Path, logger: logging.Logger) -> pd.DataFrame:
    try:
        with file_path.open("rb") as f:
            df = pd.read_csv(
                f,
                encoding="latin1",
                low_memory=False,
                on_bad_lines="warn",
                dtype_backend="pyarrow"
            )
        df.columns = [col.replace('\ufeff', '').replace('ï»¿', '').strip('"') for col in df.columns]
        return df
    except pd.errors.EmptyDataError:
        logger.error(f"Empty file: {file_path.name}")
        return pd.DataFrame()
    except Exception as e:
        logger.error(f"Failed to load {file_path.name}: {str(e)}", exc_info=True)
        return pd.DataFrame()

# 🧵 Parallel wrapper
def _read_and_store(file_path: Path, folder_name: str, logger: logging.Logger) -> Optional[Tuple[str, pd.DataFrame]]:
    key = file_path.stem
    df = read_csv_file(file_path, logger)
    if df.empty:
        logger.warning(f"Empty DataFrame: {file_path.name}")
        return None
    logger.info(f"[Parallel] Loaded {key} from {folder_name}")
    return key, df

# 📂 Folder-wise CSV ingestion
def load_csv_files(
    logger: logging.Logger,
    main_folder: str,
    subfolders: List[str],
    use_parallel: bool = False,
    max_workers: int = 4
) -> Dict[str, Dict[str, pd.DataFrame]]:
    dataframes: Dict[str, Dict[str, pd.DataFrame]] = {}

    for folder_name in subfolders:
        folder_path = Path(main_folder) / folder_name
        if not folder_path.exists():
            logger.warning(f"Missing folder: {folder_path.resolve()}")
            continue

        csv_files = list(folder_path.glob("*.csv"))
        if not csv_files:
            logger.warning(f"No CSV files in: {folder_path.resolve()}")
            continue

        dataframes[folder_name] = {}
        if use_parallel:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                task = partial(_read_and_store, folder_name=folder_name, logger=logger)
                for result in executor.map(task, csv_files):
                    if result:
                        key, df = result
                        dataframes[folder_name][key] = df
        else:
            for file_path in csv_files:
                key = file_path.stem
                df = read_csv_file(file_path, logger)
                if df.empty:
                    continue
                dataframes[folder_name][key] = df
                logger.info(
                    f"Loaded {key} from {folder_name} "
                    f"({len(df):,} rows, {df.memory_usage().sum()/1024/1024:.2f} MB)"
                )

    return dataframes

# 🔍 Validation utility
# Validates folder path and checks for suspicious segments
def validate_folder_path(config_path: str, logger: logging.Logger) -> None:
    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    folder_raw = config.get("folder", "")
    folder_path = Path(folder_raw)

    # Catch suspicious starting segments
    suspicious = ["..", "...", ".", "/", "\\"]

    if any(folder_raw.strip().startswith(s) for s in suspicious):
        logger.warning(
            f"⚠️ Suspicious folder path in config: '{folder_raw}' — "
            "consider using a relative path like 'data/raw' from the project root."
        )

    # Absolute check
    if folder_path.is_absolute():
        logger.warning(
            f"⚠️ Absolute folder path detected: '{folder_path}' — "
            "pipeline works best with relative paths to ensure portability."
        )
# 📂 Validate data sources
def validate_data_sources(config_path: str, logger: logging.Logger) -> None:
    project_root = Path(__file__).resolve().parents[1]

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    data_root = project_root / config["folder"]
    subfolders = config.get("subfolders", [])

    logger.info(" Validating data sources...\n")

    for subfolder in subfolders:
        folder_path = data_root / subfolder
        if not folder_path.exists():
            logger.warning(f" Missing folder: {folder_path.resolve()}")
            continue

        csv_files = list(folder_path.glob("*.csv"))
        count = len(csv_files)
        if count == 0:
            logger.warning(f" Folder exists but contains no CSVs: {folder_path.resolve()}")
        else:
            logger.info(f" Found {count:,} CSV file(s) in: {folder_path.resolve()}")

# 🚪 Entry point for pipeline ingestion
def load_dataframes(config_path: str, logger: logging.Logger) -> Dict[str, Dict[str, pd.DataFrame]]:
    project_root = Path(__file__).resolve().parents[1]

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    data_root = project_root / config["folder"]

    return load_csv_files(
        logger=logger,
        main_folder=str(data_root),
        subfolders=config["subfolders"],
        use_parallel=config.get("parallel", False),
        max_workers=config.get("workers", 4)
    )
