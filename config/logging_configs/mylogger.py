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
import logging.handlers
import os
import queue
import atexit
from pathlib import Path
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
        return record.levelno <= logging.INFO


class DroppingQueue(queue.Queue):
    """
    A bounded queue that discards new items when full 
    instead of blocking the application thread.
    """
    def put(self, item, block=True, timeout=None):
        try:
            # Force non-blocking put to catch the Full exception instantly
            super().put(item, block=False)
        except queue.Full:
            # Silently drop the log line to protect system stability.
            # Alternatively, write a single emergency message directly to sys.__stderr__
            pass

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
    
    # 4. Resolve the QueueHandler and manually bind its background QueueListener thread
    root_logger = logging.getLogger()
    
    # Find all target handlers configured in the root logger via standard dictionary lookup
    # dictConfig will successfully instantiate the base handlers, but we must link them to the Queue
    all_handlers = {h.name if hasattr(h, 'name') else name: h for name, h in logging._handlers.items()}
    
    for handler in root_logger.handlers:
        if isinstance(handler, logging.handlers.QueueHandler):

            # Enforce a strict upper bound of 50,000 log entries in memory.
            # At ~300 bytes per structured JSON log, this caps memory usage at ~15MB.
            bounded_memory_queue = DroppingQueue(maxsize=50000)
            
            # Swapping out the standard unbounded queue with our safe bounded version
            handler.queue = bounded_memory_queue

            # Check if dictConfig already set up a listener (rare in standard implementations)
            if not hasattr(handler, "listener"):
                # Manually extract the child handlers that dictConfig attached to the system
                # or extract them directly from the logging module's initialized pool
                stdout_handler = logging._handlers.get("stdout")
                stderr_handler = logging._handlers.get("stderr")
                file_handler = logging._handlers.get("file_json")

                targets = [h for h in [stdout_handler, stderr_handler, file_handler] if h is not None]
                
                # Bind a fresh standard QueueListener mapping the memory queue to the physical handlers
                handler.listener = logging.handlers.QueueListener(
                    handler.queue, 
                    *targets, 
                    respect_handler_level=True
                )

            # Safely spin up the dedicated lightweight background thread
            handler.listener.start() 

            # Register an exit hook to flush remaining queue messages smoothly on application shutdown
            atexit.register(handler.listener.stop)
