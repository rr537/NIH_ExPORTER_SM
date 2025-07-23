import logging
from pathlib import Path
from typing import Optional
from datetime import datetime
from pathlib import Path
import logging
from typing import Optional

def configure_logger(
    name: str = __name__,
    config: Optional[dict] = None,
    loglevel: str = "INFO",
    log_file: str = None,
    log_to_file: bool = True,
    log_to_console: bool = True,
    file_mode: str = "w"
) -> logging.Logger:
    """
    Creates and configures a logger with file and/or console output.
    If no log_file is provided, logs are written to 'logs/nih_export_log.txt'.
    """

    # # 🧭 Determine base directory
    # base_dir = Path(config.get("output_dir", ".")).resolve() if config else Path.cwd()

    # 📁 Ensure logs/ folder exists
    logs_dir = Path(config.get("logs_dir", "logs")).resolve()
    logs_dir.mkdir(parents=True, exist_ok=True)

    # 🕒 Add timestamp to filename
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    default_name = config.get("log_file", "nih_export_log.txt") if config else "nih_export_log.txt"
    default_stamped = f"{Path(default_name).stem}_{timestamp}.log"
    full_log_path = Path(log_file) if log_file else logs_dir / default_stamped

    # ⚙️ Configure logger
    logger = logging.getLogger(name)
    level = getattr(logging, loglevel.upper(), logging.INFO)
    logger.setLevel(level)

    if logger.hasHandlers():
        logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")

    if log_to_file:
        file_handler = logging.FileHandler(full_log_path, mode=file_mode, encoding="utf-8")
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if log_to_console:
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger