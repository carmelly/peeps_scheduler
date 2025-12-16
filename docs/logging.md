# Logging Configuration and Usage

This document describes the logging infrastructure for the Peeps Scheduler project.

## Overview

The project uses a centralized logging system with organized directory structure, daily rotation, and configurable retention policies. All logging is configured through the `logging_config.py` module.

## Log Directory Structure

Logs are organized into subdirectories under `/logs`:

```
/logs/
├── import/          # Import script logs (db/import_period_data.py)
│   └── import_YYYY-MM-DD.log
├── scheduler/       # Scheduler execution logs (scheduler.py, main.py scheduler commands)
│   └── scheduler_YYYY-MM-DD.log
├── validation/      # Validation command logs (future validation CLI tools)
│   └── validation_YYYY-MM-DD.log
├── app/            # General application logs (catch-all)
│   └── app_YYYY-MM-DD.log
└── cli/            # CLI command logs (main.py commands, other CLI tools)
    └── cli_YYYY-MM-DD.log
```

## Log Format

All logs use a standardized format:

```
YYYY-MM-DD HH:MM:SS,mmm - logger_name - LEVEL - message
```

Example:
```
2025-12-15 14:30:45,123 - import_csv - INFO - Processing period 2025-03
2025-12-15 14:30:46,456 - scheduler - DEBUG - Evaluating 5040 total sequences
```

## Log Rotation and Retention

### Daily Rotation

Logs automatically rotate daily at midnight. Each day's logs are stored in a separate file with the date in the filename:
- `import_2025-12-15.log`
- `import_2025-12-16.log`
- etc.

### Retention Policy

By default, logs are retained for **30 days**. Older logs are automatically deleted during logger initialization.

To change the retention period, set the `LOG_RETENTION_DAYS` environment variable:

```bash
export LOG_RETENTION_DAYS=60  # Keep logs for 60 days
```

### Size-Based Rotation

For high-volume operations, size-based rotation is available (configured per logger):
- Maximum file size: 10MB (configurable via `MAX_LOG_SIZE_MB` environment variable)
- Backup count: 5 files
- File naming: `{log_subdir}_rolling.log`, `{log_subdir}_rolling.log.1`, etc.

## Log Levels

The project uses standard Python logging levels:

| Level    | Numeric Value | Usage |
|----------|---------------|-------|
| DEBUG    | 10            | Detailed information for debugging (e.g., permutation counts, intermediate calculations) |
| INFO     | 20            | General informational messages (e.g., "Processing period 2025-03", "Import complete") |
| WARNING  | 30            | Warning messages for unexpected situations that don't prevent execution |
| ERROR    | 40            | Error messages for failures that prevent specific operations |
| CRITICAL | 50            | Critical errors that may cause the application to abort |

### When to Use Each Level

- **DEBUG**: Use for detailed diagnostic information useful during development and troubleshooting
  - Example: `logger.debug(f"Evaluating {len(sequences)} total sequences")`
- **INFO**: Use for important operational events that confirm normal behavior
  - Example: `logger.info("Processing period 2025-03")`
- **WARNING**: Use for recoverable issues or unexpected situations
  - Example: `logger.warning("Responses file not found - priority will not be updated")`
- **ERROR**: Use for errors that prevent specific operations from completing
  - Example: `logger.error(f"Database connection failed: {e}")`
- **CRITICAL**: Use for severe errors that may cause application termination
  - Example: `logger.critical("Database schema validation failed - cannot proceed")`

## Configuration

### Environment Variables

The logging system can be configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `INFO` | Default log level (DEBUG, INFO, WARNING, ERROR, CRITICAL) |
| `LOG_RETENTION_DAYS` | `30` | Number of days to retain logs |
| `MAX_LOG_SIZE_MB` | `10` | Maximum log file size for size-based rotation (in MB) |

### Setting Environment Variables

**Linux/macOS:**
```bash
export LOG_LEVEL=DEBUG
export LOG_RETENTION_DAYS=60
export MAX_LOG_SIZE_MB=20
```

**Windows (Command Prompt):**
```cmd
set LOG_LEVEL=DEBUG
set LOG_RETENTION_DAYS=60
set MAX_LOG_SIZE_MB=20
```

**Windows (PowerShell):**
```powershell
$env:LOG_LEVEL="DEBUG"
$env:LOG_RETENTION_DAYS="60"
$env:MAX_LOG_SIZE_MB="20"
```

### Command-Line Verbose Flag

Most CLI commands support a `--verbose` flag to enable DEBUG-level logging:

```bash
# Import with verbose logging
python db/import_period_data.py --period 2025-02 --verbose

# Run scheduler with verbose logging
python main.py run --data-folder data/2025-02 --verbose
```

## Usage in Code

### Basic Usage

```python
from logging_config import get_logger

# Get a logger for your module
logger = get_logger('my_module', 'app')

# Log messages
logger.debug('Detailed debug information')
logger.info('Normal operational message')
logger.warning('Warning message')
logger.error('Error message')
logger.critical('Critical error')
```

### Logger Parameters

The `get_logger()` function accepts the following parameters:

```python
def get_logger(
    name: str,                    # Logger name (e.g., 'import_csv', 'scheduler')
    log_subdir: str,              # Subdirectory under /logs (e.g., 'import', 'scheduler')
    level: Optional[str] = None,  # Log level (default: from LOG_LEVEL env var or 'INFO')
    use_size_rotation: bool = False,  # Enable size-based rotation
    console_output: bool = True   # Also output to console (default: True)
) -> logging.Logger:
```

### Example: High-Volume Operation with Size Rotation

```python
from logging_config import get_logger

# For operations that generate large amounts of logs
logger = get_logger(
    'bulk_import',
    'import',
    level='DEBUG',
    use_size_rotation=True  # Enable size-based rotation in addition to daily
)
```

### Example: Background Task (No Console Output)

```python
from logging_config import get_logger

# For background tasks that shouldn't clutter the console
logger = get_logger(
    'background_job',
    'app',
    console_output=False  # Only log to file, not console
)
```

## Migrating from Old Logging System

The old logging system used a single `debug.log` file in the project root. This has been replaced with the organized `/logs` directory structure.

### Changes Required for Existing Code

**Old approach:**
```python
import logging

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
```

**New approach:**
```python
from logging_config import get_logger

logger = get_logger('my_module', 'app')
```

### Files Updated

The following files have been updated to use the new logging system:
- `db/import_period_data.py` - Uses `/logs/import/`
- `scheduler.py` - Uses `/logs/scheduler/`
- `main.py` - Uses `/logs/cli/`
- `utils.py` - Uses `/logs/cli/`

### Deprecation Notice

The old `debug.log` file is **deprecated** and will no longer be created by new code. Existing `debug.log` references should be updated to use the new logging system.

## Troubleshooting

### Logs Not Appearing

1. **Check log directory exists**: The `/logs` directory and subdirectories are created automatically, but verify they exist.
2. **Check log level**: If you're not seeing DEBUG messages, ensure the log level is set to DEBUG:
   ```bash
   python db/import_period_data.py --period 2025-02 --verbose
   ```
3. **Check permissions**: Ensure the application has write permissions to the `/logs` directory.

### Log Files Growing Too Large

If individual log files are growing too large:

1. **Enable size-based rotation** for high-volume loggers:
   ```python
   logger = get_logger('my_module', 'app', use_size_rotation=True)
   ```

2. **Reduce log retention period**:
   ```bash
   export LOG_RETENTION_DAYS=7  # Keep only 1 week of logs
   ```

3. **Increase log level** to reduce verbosity:
   ```bash
   export LOG_LEVEL=WARNING  # Only log warnings and errors
   ```

### Old Logs Not Being Cleaned Up

Logs are cleaned up during logger initialization. If old logs are not being removed:

1. **Check retention policy**: Verify `LOG_RETENTION_DAYS` is set correctly.
2. **Restart application**: Old logs are cleaned up on logger initialization, so restart the application to trigger cleanup.
3. **Manual cleanup**: You can manually delete old logs from `/logs` subdirectories.

### Duplicate Log Messages

If you're seeing duplicate log messages, ensure you're not creating multiple loggers with the same name. The `get_logger()` function prevents duplicate handlers, but if you're using `logging.getLogger()` directly, you may encounter duplicates.

**Solution:** Always use `get_logger()` from `logging_config.py` instead of `logging.getLogger()`.

## Best Practices

1. **Use appropriate log levels**: Don't log everything at INFO level. Use DEBUG for detailed diagnostics, INFO for operational events, WARNING/ERROR/CRITICAL for problems.

2. **Include context in log messages**: Include relevant identifiers (period names, peep IDs, event IDs) in log messages:
   ```python
   logger.info(f"Processing period {period_name}")
   logger.error(f"Failed to import peep {peep_id}: {error}")
   ```

3. **Log exceptions with stack traces**:
   ```python
   try:
       # ... code ...
   except Exception as e:
       logger.error(f"Import failed: {e}", exc_info=True)  # exc_info=True includes stack trace
   ```

4. **Don't log sensitive information**: Avoid logging passwords, API keys, or other sensitive data.

5. **Use structured logging for complex data**: For complex data structures, use JSON formatting:
   ```python
   import json
   logger.debug(f"Event data: {json.dumps(event_dict, indent=2)}")
   ```

6. **Monitor log file sizes**: Regularly check log file sizes and adjust retention/rotation policies as needed.

## Version Control

The `/logs` directory is excluded from version control via `.gitignore`. The directory structure is created automatically by `logging_config.py` when needed.

**`.gitignore` entries:**
```gitignore
# Logs directory (new logging infrastructure - created automatically)
/logs/
*.log
```

## Future Enhancements

Potential future improvements to the logging system:

- **Structured logging**: JSON-formatted logs for easier parsing and analysis
- **Log aggregation**: Integration with log aggregation services (e.g., ELK stack, CloudWatch)
- **Performance metrics**: Automatic logging of performance metrics (execution time, memory usage)
- **Log compression**: Automatic compression of old log files to save disk space
- **Remote logging**: Send logs to remote logging services for centralized monitoring
