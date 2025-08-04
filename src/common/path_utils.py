from pathlib import Path
import logging
from typing import Optional, Union, Dict

#  Define valid stages
VALID_STAGES = ["preprocess", "metrics", "keywords", "finalize"]

def resolve_output_path(
    stage: str,
    output_path: Optional[Union[str, Path]],
    config: Optional[Dict],
    logger: Optional[logging.Logger] = None
) -> Path:
    """
    Resolves and ensures output directory for a stage. Uses config[stage+'_dir'], or falls back to output_dir/stage.
    """
    if stage not in VALID_STAGES:
        raise ValueError(f"Unknown stage: '{stage}' — valid options: {VALID_STAGES}")

    # Optional override
    if output_path:
        resolved_path = Path(output_path).resolve()
        if logger:
            logger.info(f"📤 Using explicit output path for stage '{stage}': {resolved_path}")
    else:
        # Construct config key and fallback
        stage_key = f"{stage}_dir"
        root_dir = Path(config.get("output_dir", "results")).resolve()
        stage_path = config.get(stage_key, root_dir / stage)
        resolved_path = Path(stage_path).resolve()
        if logger:
            logger.info(f"📤 Using config/default output path for stage '{stage}': {resolved_path}")

    resolved_path.mkdir(parents=True, exist_ok=True)
    return resolved_path

INPUT_PATHS = {
    "metrics": {
        "config_key": "preprocess_dir",
        "file_glob": "*.pkl"
    },
    "keywords": {
        "config_key": "metrics_dir",
        "file_name": "metrics.csv"
    },
    "finalize": {
        "config_key": "keywords_dir",
        "file_name": "keywords.csv"
    }
}

def resolve_input_path(stage: str, input_path: Optional[Union[str, Path]], config: Dict, logger=None) -> Path:
    stage_map = INPUT_PATHS.get(stage)
    if not stage_map:
        raise ValueError(f"Unknown stage: {stage}")

    if input_path:
        resolved = Path(input_path).resolve()
        if logger: logger.info(f"Using explicit input path for '{stage}': {resolved}")
        return resolved

    config_key = stage_map["config_key"]
    output_root = Path(config.get("output_dir", "results")).resolve()
    stage_path = Path(config.get(config_key, output_root / config_key)).resolve()

    if logger: logger.info(f"Using config path for '{stage}': {stage_path}")
    return stage_path

def resolve_input_files(
    stage: str,
    input_path: Optional[Union[str, Path]],
    config: Optional[Dict],
    logger: Optional[logging.Logger] = None
) -> Union[Dict[str, Path], Path]:
    """
    Resolves either:
    - a directory of pickle files (for 'metrics')
    - a specific CSV file (for 'keywords' or 'finalize')
    """
    input_dir = resolve_input_path(stage, input_path, config, logger)
    stage_map = INPUT_PATHS.get(stage)

    # Multiple pickles (metrics)
    if "file_glob" in stage_map:
        file_paths = sorted(input_dir.glob(stage_map["file_glob"]))
        return {p.stem: p for p in file_paths}

    # Single CSV file (keywords/finalize)
    elif "file_name" in stage_map:
        file_path = input_dir / stage_map["file_name"]
        if logger:
            logger.info(f"Resolved input file for '{stage}': {file_path}")
        return file_path

    raise ValueError(f"Incomplete path logic for stage: '{stage}'")