# This Python file provides a custom structured JSON logging tool by extending 
# the built-in Python logging module. It is designed to format standard log 
# records into structured, queryable JSON data. A common requirement in modern 
# production environments that use log collectors like Datadog, Elasticsearch, or 
# AWS CloudWatch.
#
# Additionally we add a bootstrapping utility function to decouple logging setup
# from the application found in src/
import datetime as dt
import json
import logging
import logging.config
import os
import atexit
from pathlib import Path
import sys
from typing import Any, Final, override

# Built-in attributes to ignore when parsing extra fields.
# Python automatically injects these into a log event.
LOG_RECORD_BUILTIN_ATTRS: Final[set[str]] = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
    "taskName",
}


class AppJSONFormatter(logging.Formatter):
    """
    A custom formatter that parses a log event, isolates application metadata,
    maps fields to customized JSON keys, and returns a structured JSON string.
    """
    def __init__(
        self,
        *args: str,
        fmt_keys: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        """Converts a logging.LogRecord object to a structured JSON string."""
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord) -> dict[str, Any]:
        """Maps log fields to configured keys and appends exception traces."""
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }

        # Build primary map using specified schema keys from configuration
        message = {}
        for key, val in self.fmt_keys.items():
            if val in always_fields:
                message[key] = always_fields[val]
            elif hasattr(record, val):
                message[key] = getattr(record, val)

        # Inject Tracebacks cleanly under modern query keys (works for critical + exc_info=True)
        if record.exc_info:
            message["exception"] = self.formatException(record.exc_info)
        if record.stack_info:
            message["stack_trace"] = self.formatStack(record.stack_info)

        # Append extra metadata parameters seamlessly (e.g. logger.info("...", extra={...}))
        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                message[key] = val

        return message


class NonErrorFilter(logging.Filter):
    """Custom logging filter allowing only INFO or lower severity logs to pass."""
    @override
    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno <= logging.WARNING


def setup_production_logging(config_path: str | None = None) -> None:
    """
    Loads JSON logging configuration, explicitly links and manages the internal 
    QueueListener thread to decouple blocking I/O from high-throughput application loops.
    """
    # 1. Resolve paths deterministically using modern Pathlib
    # Assumes this script lives in: repo_root/config/logging_configs/mylogger.py
    current_file = Path(__file__).resolve()
    config_dir = current_file.parent  # config/logging_configs/
    project_root = current_file.parents[2]  # Climbs up 3 levels to the true repo root

    if config_path is None:
        # Default fallback location if no custom path provided
        config_path = str(config_dir / "logger_configuration.json")

    logs_dir = project_root / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)

    # 2. Load and sanitize configuration file
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Logging configuration file not found at: {config_path}")

    with open(config_path, "r") as f:
        config_dict = json.load(f)

    # Force the RotatingFileHandler to use an absolute path targeting our verified logs directory
    absolute_log_path = str(logs_dir / "app_log.jsonl")
    if "handlers" in config_dict and "file_json" in config_dict["handlers"]:
        config_dict["handlers"]["file_json"]["filename"] = absolute_log_path

    # 3. Apply dictionary configuration
    logging.config.dictConfig(config_dict)

    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)

    # Verifiy absolute bath to the log files
    sys.__stdout__.write(f"CRITICAL PATH CHECK: Logs targeted at: {os.path.abspath(absolute_log_path)}\n")
    sys.__stdout__.flush()
