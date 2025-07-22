import yaml
from pathlib import Path
import logging

def load_config(config_path: str) -> dict:
    config_file = Path(config_path).resolve()
    if not config_file.exists():
        raise FileNotFoundError(f"Config file not found at: {config_file}")
    
    with open(config_file, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    logging.getLogger(__name__).info(f"✅ Config loaded from: {config_file}")
    return config
