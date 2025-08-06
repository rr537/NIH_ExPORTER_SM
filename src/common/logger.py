from pathlib import Path
from datetime import datetime
from typing import Optional, Dict
import logging

def _resolve_log_path(config: Optional[Dict], log_file: Optional[str], timestamp: str) -> Path:
    logs_dir = Path(config.get("logs_dir", "logs")).resolve() if config else Path("logs").resolve()
    logs_dir.mkdir(parents=True, exist_ok=True)

    default_name = config.get("log_file", "nih_export_log.txt") if config else "nih_export_log.txt"
    stamped_name = f"{Path(default_name).stem}_{timestamp}.log"
    return Path(log_file) if log_file else logs_dir / stamped_name

def _create_handler(handler_type: str, log_path: Optional[Path], formatter: logging.Formatter, file_mode: str = "w") -> logging.Handler:
    if handler_type == "file" and log_path:
        handler = logging.FileHandler(log_path, mode=file_mode, encoding="utf-8")
    elif handler_type == "console":
        handler = logging.StreamHandler()
    else:
        raise ValueError("Invalid handler type")
    
    handler.setFormatter(formatter)
    return handler

def configure_logger(
    name: str = __name__,
    config: Optional[Dict] = None,
    loglevel: str = "INFO",
    log_file: Optional[str] = None,
    log_to_file: bool = True,
    log_to_console: bool = True,
    file_mode: str = "w"
) -> logging.Logger:
    """
    Configures a logger that supports console and/or file output.
    Creates timestamped log files unless a specific log_file is provided.
    """

    config = config or {}

    # Override parameters from config if available
    loglevel = config.get("loglevel", loglevel)
    log_file = config.get("log_file", log_file)
    log_to_file = config.get("log_to_file", log_to_file)
    log_to_console = config.get("log_to_console", log_to_console)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    log_path = _resolve_log_path(config, log_file, timestamp)

    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, loglevel.upper(), logging.INFO))

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if log_to_file:
        logger.addHandler(_create_handler("file", log_path, formatter, file_mode))

    if log_to_console:
        logger.addHandler(_create_handler("console", None, formatter))

    return logger
