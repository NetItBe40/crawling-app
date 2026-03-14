"""
Logging configuration for all modules
"""
import logging
import sys
from logging.handlers import RotatingFileHandler
from config.settings import LOG_DIR


def setup_logger(name: str, log_file: str = None, level=logging.INFO) -> logging.Logger:
    """Create a logger with console and optional file handler."""
    logger = logging.getLogger(name)
    logger.setLevel(level)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s | %(name)-20s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Console handler
    console = logging.StreamHandler(sys.stdout)
    console.setLevel(level)
    console.setFormatter(formatter)
    logger.addHandler(console)

    # File handler
    if log_file:
        file_path = LOG_DIR / log_file
        file_handler = RotatingFileHandler(
            file_path, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger
