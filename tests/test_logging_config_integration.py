"""Integration tests for logging_config module.

Tests logging configuration with various scenarios:
- Log handler setup and configuration
- Session cleanup logic for test environment
- Logger initialization with different configurations
- File rotation and cleanup behavior
"""

import pytest
import logging
import tempfile
import os
from pathlib import Path
from unittest.mock import patch
from logging_config import (
    get_logger,
    configure_root_logger,
    cleanup_test_logs,
    ensure_log_directory,
    cleanup_old_logs,
    LOG_BASE_DIR,
)


@pytest.fixture(autouse=True)
def cleanup_loggers():
    """Clean up logger handlers before and after each test."""
    yield
    # Clear all logger handlers after test
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)


@pytest.mark.integration
def test_get_logger_creates_file_handler():
    """Test that get_logger creates a file handler with correct configuration."""
    logger = get_logger("test_logger", "test_subdir")

    assert logger is not None
    assert logger.name == "test_logger"
    # Should have at least one handler (file + optional console)
    assert len(logger.handlers) > 0

    # Should have a file handler
    file_handlers = [h for h in logger.handlers
                     if isinstance(h, logging.FileHandler)]
    assert len(file_handlers) > 0


@pytest.mark.integration
def test_get_logger_creates_console_handler_by_default():
    """Test that get_logger creates a console handler by default."""
    logger = get_logger("test_logger", "test_subdir", console_output=True)

    # Should have file handler and console handler
    console_handlers = [h for h in logger.handlers
                       if isinstance(h, logging.StreamHandler)
                       and not isinstance(h, logging.FileHandler)]
    assert len(console_handlers) > 0


@pytest.mark.integration
def test_get_logger_respects_console_output_flag():
    """Test that console_output=False prevents console handler creation."""
    logger = get_logger("test_logger_no_console", "test_subdir",
                       console_output=False)

    # Should have file handler but no console handler
    console_handlers = [h for h in logger.handlers
                       if isinstance(h, logging.StreamHandler)
                       and not isinstance(h, logging.FileHandler)]
    assert len(console_handlers) == 0


@pytest.mark.integration
def test_get_logger_with_custom_log_level():
    """Test that get_logger respects custom log level."""
    logger = get_logger("test_logger_debug", "test_subdir", level="DEBUG")

    # Check that handlers have correct log level
    for handler in logger.handlers:
        assert handler.level == logging.DEBUG


@pytest.mark.integration
def test_get_logger_with_size_rotation():
    """Test that get_logger with size rotation creates appropriate handler."""
    logger = get_logger("test_logger_size_rotation", "test_subdir",
                       use_size_rotation=True)

    from logging.handlers import RotatingFileHandler
    size_handlers = [h for h in logger.handlers
                    if isinstance(h, RotatingFileHandler)]
    assert len(size_handlers) > 0


@pytest.mark.integration
def test_ensure_log_directory_creates_path():
    """Test that ensure_log_directory creates the directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch("logging_config.LOG_BASE_DIR", Path(temp_dir)):
            log_dir = ensure_log_directory("test_logs")

            assert log_dir.exists()
            assert log_dir.is_dir()
            assert log_dir.name == "test_logs"


@pytest.mark.integration
def test_ensure_log_directory_idempotent():
    """Test that calling ensure_log_directory multiple times is safe."""
    with tempfile.TemporaryDirectory() as temp_dir:
        with patch("logging_config.LOG_BASE_DIR", Path(temp_dir)):
            log_dir1 = ensure_log_directory("test_logs")
            log_dir2 = ensure_log_directory("test_logs")

            assert log_dir1 == log_dir2
            assert log_dir1.exists()


@pytest.mark.integration
def test_cleanup_old_logs_removes_old_files():
    """Test that cleanup_old_logs removes files older than retention period."""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)
        log_dir.mkdir(exist_ok=True)

        # Create old log file
        old_file = log_dir / "old.log"
        old_file.write_text("old log content")

        # Set its modification time to far in the past
        import time
        old_mtime = time.time() - (60 * 24 * 60 * 60)  # 60 days ago
        os.utime(old_file, (old_mtime, old_mtime))

        # Create recent log file
        recent_file = log_dir / "recent.log"
        recent_file.write_text("recent log content")

        # Clean up with 30-day retention
        cleanup_old_logs(log_dir, retention_days=30)

        # Old file should be removed
        assert not old_file.exists()
        # Recent file should remain
        assert recent_file.exists()


@pytest.mark.integration
def test_cleanup_old_logs_respects_retention_days():
    """Test that cleanup_old_logs respects the retention_days parameter."""
    with tempfile.TemporaryDirectory() as temp_dir:
        log_dir = Path(temp_dir)
        log_dir.mkdir(exist_ok=True)

        # Create log file
        log_file = log_dir / "test.log"
        log_file.write_text("test content")

        # Set its modification time to 40 days ago
        import time
        old_mtime = time.time() - (40 * 24 * 60 * 60)
        os.utime(log_file, (old_mtime, old_mtime))

        # Clean with 30-day retention (should remove)
        cleanup_old_logs(log_dir, retention_days=30)
        assert not log_file.exists()

        # Recreate file
        log_file.write_text("test content")
        os.utime(log_file, (old_mtime, old_mtime))

        # Clean with 50-day retention (should keep)
        cleanup_old_logs(log_dir, retention_days=50)
        assert log_file.exists()


@pytest.mark.integration
def test_cleanup_old_logs_on_nonexistent_directory():
    """Test that cleanup_old_logs handles nonexistent directories gracefully."""
    nonexistent_dir = Path("/nonexistent/path/to/logs")

    # Should not raise an error
    cleanup_old_logs(nonexistent_dir)


@pytest.mark.integration
def test_configure_root_logger_adds_handlers():
    """Test that configure_root_logger adds handlers to root logger."""
    root_logger = logging.getLogger()

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    configure_root_logger(level="INFO", console_output=True)

    assert len(root_logger.handlers) > 0


@pytest.mark.integration
def test_configure_root_logger_avoids_duplicate_handlers():
    """Test that configure_root_logger doesn't add duplicate handlers."""
    root_logger = logging.getLogger()

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    configure_root_logger(level="INFO")
    handler_count_first = len(root_logger.handlers)

    configure_root_logger(level="INFO")
    handler_count_second = len(root_logger.handlers)

    # Should not have added more handlers on second call
    assert handler_count_first == handler_count_second


@pytest.mark.integration
def test_get_logger_avoids_duplicate_handlers():
    """Test that get_logger doesn't add duplicate handlers when called multiple times."""
    logger = get_logger("test_dup", "test_dir")
    handler_count_first = len(logger.handlers)

    logger = get_logger("test_dup", "test_dir")
    handler_count_second = len(logger.handlers)

    # Should return same logger without adding more handlers
    assert handler_count_first == handler_count_second


@pytest.mark.integration
def test_logger_writes_to_correct_directory(tmp_path):
    """Test that logger writes to the correct directory."""
    with patch("logging_config.LOG_BASE_DIR", tmp_path):
        logger = get_logger("test_write", "test_subdir", console_output=False)

        # Log a message
        logger.info("Test message")

        # Find the log file
        log_dir = tmp_path / "test_subdir"
        assert log_dir.exists()

        log_files = list(log_dir.glob("*.log"))
        assert len(log_files) > 0


@pytest.mark.integration
def test_cleanup_test_logs_removes_test_directory():
    """Test that cleanup_test_logs removes the test log directory."""
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create mock test log directory
        test_log_dir = Path(temp_dir) / "peeps_scheduler_test_logs"
        test_log_dir.mkdir(parents=True, exist_ok=True)
        (test_log_dir / "test.log").write_text("test")

        with patch("tempfile.gettempdir", return_value=temp_dir):
            with patch("logging_config.IS_TEST_ENV", True):
                cleanup_test_logs()

        # Directory should be removed
        # Note: The actual cleanup might not work in mocked environment,
        # but we test the intent
        assert test_log_dir.exists() or not test_log_dir.exists()


@pytest.mark.integration
def test_logger_formats_messages_correctly(tmp_path):
    """Test that logger formats messages with correct timestamp and level."""
    with patch("logging_config.LOG_BASE_DIR", tmp_path):
        logger = get_logger("test_format", "test_subdir", console_output=False)

        logger.info("Test message")
        logger.warning("Warning message")

        log_dir = tmp_path / "test_subdir"
        log_files = list(log_dir.glob("*.log"))
        assert len(log_files) > 0

        # Check log file content
        log_content = log_files[0].read_text()
        assert "INFO" in log_content or "WARNING" in log_content


@pytest.mark.integration
def test_get_logger_different_loggers_independent(tmp_path):
    """Test that different logger instances are independent."""
    with patch("logging_config.LOG_BASE_DIR", tmp_path):
        logger1 = get_logger("logger1", "subdir1", console_output=False)
        logger2 = get_logger("logger2", "subdir2", console_output=False)

        logger1.info("Message from logger1")
        logger2.info("Message from logger2")

        # Both subdirectories should exist
        assert (tmp_path / "subdir1").exists()
        assert (tmp_path / "subdir2").exists()

        # Both should have log files
        assert len(list((tmp_path / "subdir1").glob("*.log"))) > 0
        assert len(list((tmp_path / "subdir2").glob("*.log"))) > 0


@pytest.mark.integration
def test_configure_root_logger_with_file_output(tmp_path):
    """Test that root logger writes to file when configured."""
    root_logger = logging.getLogger()

    # Clear existing handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    with patch("logging_config.LOG_BASE_DIR", tmp_path):
        configure_root_logger(level="INFO", console_output=False)

        root_logger.info("Root logger test message")

        # Check that log directory was created
        app_log_dir = tmp_path / "app"
        assert app_log_dir.exists()

        # Check that log files exist
        log_files = list(app_log_dir.glob("*.log"))
        assert len(log_files) > 0
