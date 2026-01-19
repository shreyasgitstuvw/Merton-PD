# src/utils/logger.py
"""
Centralized logging configuration for Merton PD engine.
"""

import logging
import sys
from pathlib import Path
from datetime import datetime


def setup_logger(
        name: str = "merton",
        level: int = logging.INFO,
        log_file: str = None
) -> logging.Logger:
    """
    Setup logger with console and optional file output.

    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_file: Optional log file path

    Returns:
        Configured logger
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Console handler with formatting
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)

    # Detailed format
    formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Optional file handler
    if log_file:
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# Default logger instance
default_logger = setup_logger()


def get_logger(name: str = None) -> logging.Logger:
    """
    Get logger instance.

    Args:
        name: Logger name (will be prefixed with 'merton.')

    Returns:
        Logger instance
    """
    if name:
        return logging.getLogger(f"merton.{name}")
    return default_logger


# Example usage
if __name__ == "__main__":
    # Test the logger
    logger = setup_logger("test", log_file="logs/test.log")

    logger.debug("This is a debug message")
    logger.info("This is an info message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
