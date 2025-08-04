import pandas as pd
import logging
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Dict, List, Optional, Tuple, Any, Union

import json

# Entry point for pipeline ingestion
def ingest_dataframes(config: Dict, logger: logging.Logger) -> Tuple[Dict[str, Dict[str, pd.DataFrame]], Dict[str, Dict]]:
    data_root = Path(__file__).resolve().parents[2] / config["folder"]

    raw_dict = load_csv_files(
        logger=logger,
        main_folder=str(data_root),
        subfolders=config["subfolders"],
        use_parallel=config.get("parallel", False),
        max_workers=config.get("workers", 4)
    )

    load_summary = summarize_csv_load(raw_dict)
    return raw_dict, load_summary

# Folder-wise CSV ingestion
def load_csv_files(
    logger: logging.Logger,
    main_folder: str,
    subfolders: List[str],
    use_parallel: bool = False,
    max_workers: int = 4
) -> Dict[str, Dict[str, pd.DataFrame]]:
    dataframes = {}

    for folder_name in subfolders:
        folder_path = Path(main_folder) / folder_name
        if not folder_path.exists():
            logger.warning(f"Missing folder: {folder_path.resolve()}")
            continue

        csv_files = list(folder_path.glob("*.csv"))
        if not csv_files:
            logger.warning(f"Folder exists but contains no CSVs: {folder_path.resolve()}")
            continue

        logger.info(f"Processing {folder_name} with {len(csv_files)} CSV(s)...")
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
                logger.info(f"Loaded {key} from {folder_name} ({len(df):,} rows, {df.memory_usage().sum()/1024/1024:.2f} MB)")

    return dataframes

#  Individual file reader
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

#  Parallel wrapper
def _read_and_store(file_path: Path, folder_name: str, logger: logging.Logger) -> Optional[Tuple[str, pd.DataFrame]]:
    key = file_path.stem
    df = read_csv_file(file_path, logger)
    if df.empty:
        logger.warning(f"Empty DataFrame: {file_path.name}")
        return None
    logger.info(f"[Parallel] Loaded {key} from {folder_name}")
    return key, df

 # Collect summary of loaded DataFrames
def summarize_csv_load(dataframes: Dict[str, Dict[str, pd.DataFrame]]) -> Dict[str, Dict]:
    summary = {}
    for folder, files in dataframes.items():
        summary[folder] = {
            "folder": folder,
            "file_count": len(files),
            "total_rows": sum(df.shape[0] for df in files.values()),
            "total_memory": sum(df.memory_usage().sum() for df in files.values()) / (1024 * 1024)  # MB
        }
    return summary

# Save each DataFrame to pickle
def save_pickle_files(
    appended_dict: Dict[str, pd.DataFrame],
    output_dir: Path,
    logger: logging.Logger
) -> None:
    """
    Saves each DataFrame in the dictionary to a pickle file under resolved output_path.
    If output_path is None, resolves using config[config_key] or fallback.
    """
    logger.info(f"Saving {len(appended_dict)} pickle file(s) to: {output_dir}")

    print(" Exporting the following keys:", list(appended_dict.keys()))
    for name, df in appended_dict.items():
        path = output_dir / f"{name}.pkl"
        try:
            df.to_pickle(path)
            logger.info(f"Saved {name}.pkl to {path}")
        except Exception as e:
            logger.error(f"Failed to save {name}.pkl: {str(e)}", exc_info=True)

# Export summary dictionary to JSON
def export_summary_json(
    summary: Dict[str, Any],
    output_dir: Path,
    logger: logging.Logger,
    summary_path: Path = None
) -> None:
    """
    Exports the summary dictionary to a JSON file. If no summary_path is given,
    defaults to output_path/preprocessing_summary.json.
    """
    if summary_path is None:
        summary_path = output_dir / "preprocessing_summary.json"
    else:
        summary_path = summary_path.resolve()

    summary_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
        logger.info(f"Preprocessing summary exported to: {summary_path}")
    except Exception as e:
        logger.error(f"Failed to export summary JSON: {str(e)}", exc_info=True)