import logging
from pathlib import Path
from typing import Optional

def configure_logger(
    name: str = __name__,
    config: Optional[dict] = None,
    loglevel: str = "INFO",
    log_file: str = "nih_export_log.txt",
    log_to_file: bool = True,
    log_to_console: bool = True,
    file_mode: str = "w"
) -> logging.Logger:

    # Use config-driven output path if provided
    output_dir = Path(config.get("output_dir", ".")) if config else Path(".")
    output_dir.mkdir(parents=True, exist_ok=True)
    full_log_path = output_dir / config.get("log_file", log_file) if config else output_dir / log_file

    logger = logging.getLogger(name)
    level = getattr(logging, loglevel.upper(), logging.INFO)
    logger.setLevel(level)

    # Avoid log duplication if logger is reused
    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if log_to_file:
        file_handler = logging.FileHandler(full_log_path, mode=file_mode)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger