"""
Centralized Logging Configuration Module

Provides standardized logging setup with:
- Organized directory structure (/logs with subdirectories)
- Daily log rotation with date stamps
- Size-based rotation for high-volume operations
- Configurable retention policy (default 30 days)
- Consistent log format across all modules

Usage:
    from logging_config import get_logger

    logger = get_logger('import_csv', 'import')
    logger.info('Processing period 2025-03')
"""

import logging
import os
from datetime import datetime, timedelta
from logging.handlers import TimedRotatingFileHandler, RotatingFileHandler
from pathlib import Path
from typing import Optional


# Configuration
LOG_BASE_DIR = Path('logs')
DEFAULT_LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()
LOG_RETENTION_DAYS = int(os.getenv('LOG_RETENTION_DAYS', '30'))
MAX_LOG_SIZE_MB = int(os.getenv('MAX_LOG_SIZE_MB', '10'))

# Log format
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def ensure_log_directory(log_subdir: str) -> Path:
    """
    Ensure log directory exists.

    Args:
        log_subdir: Subdirectory under /logs (e.g., 'import', 'scheduler', 'cli')

    Returns:
        Path object for the log directory
    """
    log_dir = LOG_BASE_DIR / log_subdir
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def cleanup_old_logs(log_dir: Path, retention_days: int = LOG_RETENTION_DAYS):
    """
    Remove log files older than retention period.

    Args:
        log_dir: Directory containing log files
        retention_days: Number of days to retain logs (default from config)
    """
    if not log_dir.exists():
        return

    cutoff_date = datetime.now() - timedelta(days=retention_days)

    for log_file in log_dir.glob('*.log'):
        if log_file.stat().st_mtime < cutoff_date.timestamp():
            try:
                log_file.unlink()
            except OSError:
                pass  # Ignore errors during cleanup


def get_logger(
    name: str,
    log_subdir: str,
    level: Optional[str] = None,
    use_size_rotation: bool = False,
    console_output: bool = True
) -> logging.Logger:
    """
    Get a configured logger instance.

    Args:
        name: Logger name (e.g., 'import_csv', 'scheduler')
        log_subdir: Subdirectory under /logs (e.g., 'import', 'scheduler', 'cli')
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to DEFAULT_LOG_LEVEL
        use_size_rotation: If True, use size-based rotation (10MB) in addition to daily rotation
        console_output: If True, also output logs to console (default: True)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Avoid adding duplicate handlers
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)  # Capture all levels, handlers filter

    # Set log level
    log_level = getattr(logging, level.upper()) if level else getattr(logging, DEFAULT_LOG_LEVEL)

    # Ensure log directory exists
    log_dir = ensure_log_directory(log_subdir)

    # Clean up old logs
    cleanup_old_logs(log_dir)

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # File handler with daily rotation
    log_filename = log_dir / f"{log_subdir}_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = TimedRotatingFileHandler(
        filename=log_filename,
        when='midnight',
        interval=1,
        backupCount=LOG_RETENTION_DAYS,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # Optional: Size-based rotation for high-volume logs
    if use_size_rotation:
        size_handler = RotatingFileHandler(
            filename=log_dir / f"{log_subdir}_rolling.log",
            maxBytes=MAX_LOG_SIZE_MB * 1024 * 1024,
            backupCount=5,
            encoding='utf-8'
        )
        size_handler.setLevel(log_level)
        size_handler.setFormatter(formatter)
        logger.addHandler(size_handler)

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

    return logger


def configure_root_logger(level: Optional[str] = None, console_output: bool = True):
    """
    Configure root logger for general application logging.

    Args:
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL). Defaults to DEFAULT_LOG_LEVEL
        console_output: If True, also output logs to console (default: True)
    """
    root_logger = logging.getLogger()

    # Avoid reconfiguring if already configured
    if root_logger.handlers:
        return

    root_logger.setLevel(logging.DEBUG)

    # Set log level
    log_level = getattr(logging, level.upper()) if level else getattr(logging, DEFAULT_LOG_LEVEL)

    # Ensure log directory exists
    log_dir = ensure_log_directory('app')

    # Clean up old logs
    cleanup_old_logs(log_dir)

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT)

    # File handler with daily rotation
    log_filename = log_dir / f"app_{datetime.now().strftime('%Y-%m-%d')}.log"
    file_handler = TimedRotatingFileHandler(
        filename=log_filename,
        when='midnight',
        interval=1,
        backupCount=LOG_RETENTION_DAYS,
        encoding='utf-8'
    )
    file_handler.setLevel(log_level)
    file_handler.setFormatter(formatter)
    root_logger.addHandler(file_handler)

    # Console handler
    if console_output:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
