"""
Rotating file + console logging setup.
"""

from __future__ import annotations

import logging
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path

from motor_vehicles.config import LoggingConfig


def setup_logging(config: LoggingConfig) -> logging.Logger:
    """Configure root logger with rotating file handler and optional console."""
    logger = logging.getLogger("motor_vehicles")
    logger.setLevel(getattr(logging, config.level))

    # Clear existing handlers to avoid duplicates on reload
    logger.handlers.clear()

    fmt = logging.Formatter(
        "%(asctime)s - %(levelname)s - %(name)s - %(funcName)s:%(lineno)d - %(message)s"
    )

    # Rotating file handler
    log_path = Path(config.file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=config.rotate_mb * 1024 * 1024,
        backupCount=config.keep_backups,
        encoding="utf-8",
    )
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    # Console handler
    if config.console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(fmt)
        logger.addHandler(console_handler)

    return logger
