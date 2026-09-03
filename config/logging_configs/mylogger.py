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
from typing import override


# A set containing the names of all default properties Python automatically injects into a log event
LOG_RECORD_BUILTIN_ATTRS = {
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

# A custom formatter that parses a log event, isolates your application's 
# custom metadata, maps fields to customized JSON keys, and returns a 
# JSON string instead of plain text.

# When initializing this formatter, you can pass a dictionary called `fmt_keys`
# to map default logging names (e.g. "time": "timestamp) to keys of your choice
class AppJSONFormatter(logging.Formatter):
    def __init__(
        self,
       *args: str,
        fmt_keys: dict[str, str] | None = None,
    ) -> None:
        super().__init__()
        self.fmt_keys = fmt_keys if fmt_keys is not None else {}

    @override
    def format(self, record: logging.LogRecord) -> str:
        """
        Converts logging.Record dictionary to JSON format
        """
        message = self._prepare_log_dict(record)
        return json.dumps(message, default=str)

    def _prepare_log_dict(self, record: logging.LogRecord) -> dict[str, str | dt.datetime]:
        """
        Handles the translation of the logging message fields

        Args:
            record (logging.LogRecord): Log Record

        Returns:
            dict[str, str | dt.datetime]: the fields of the record
        """
        always_fields = {
            "message": record.getMessage(),
            "timestamp": dt.datetime.fromtimestamp(
                record.created, tz=dt.timezone.utc
            ).isoformat(),
        }
        if record.exc_info is not None:
            always_fields["exc_info"] = self.formatException(record.exc_info)

        if record.stack_info is not None:
            always_fields["stack_info"] = self.formatStack(record.stack_info)
        
        # dictionary comprehension takes the form 
        # {key: value for (key, value) in iterable if condition}
        # Note: Walrus Operator (:=) - The walrus operator let's use
        #        calculate, assign, and test a variable all on the same
        message = {
            key: msg_val
            if (msg_val := always_fields.pop(val, None)) is not None
            else getattr(record, val)
            for key, val in self.fmt_keys.items()
        }
        # takes whatever leftover key-value pairs remain in always_fields and 
        # merges them directly into the message dictionary.
        message.update(always_fields)

        # record.__dict__ is a standard Python dictionary that holds all the 
        #   attributes and values attached to the record object.

        # .items() allows the loop to iterate through these attributes as 
        #  key (the attribute name) and val (the attribute value) pairs.
        for key, val in record.__dict__.items():
            if key not in LOG_RECORD_BUILTIN_ATTRS:
                # if the attribute is not a built-in one, it means it was added dynamically.
                # Example: when using the `extra`` parameter in a log call, like below
                #   logger.info("User logged in", extra={"user_id": 42, "ip_address": "10.0.0.1"})
                message[key] = val

        return message

# A standard logging filter designed to restrict high-severity logs 
# from passing through a specific pipeline.
# Custom logging filter that only allows log messages with a severity 
# level of INFO or lower to pass through.
class NonErrorFilter(logging.Filter):
    @override
    def filter(self, record: logging.LogRecord) -> bool | logging.LogRecord:
        return record.levelno <= logging.INFO


# centralized bootstrapping utility function for logging setup
def setup_production_logging(config_path: str = None):
    """
    Loads JSON logging configuration, ensures log directories exist,
    applies dictConfig, and starts the internal QueueListener.
    """

    # If no path is provided, default relative to this file
    if config_path is None:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(current_dir, "logger_configuration.json")

    # 1. Ensure the target directory for file logs exists safely
    # should be adjacent to configs/
    current_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
    logs_dir = os.path.join(project_root, "logs")
    os.makedirs(logs_dir, exist_ok=True)
    
    # 2. Load configuration file
    with open(config_path, "r") as f:
      config_dict = json.load(f)

    # Dynamically force the RotatingFileHandler to use the correct adjacent absolute path
    absolute_log_path = os.path.join(logs_dir, "app_log.jsonl")
    config_dict["handlers"]["file_json"]["filename"] = absolute_log_path
      
    # 3. Apply dictionary configuration
    logging.config.dictConfig(config_dict)
    
    # 4. Resolve the QueueHandler and explicitly start its background thread listener
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.handlers.QueueHandler) and hasattr(handler, "listener"):

            # Spawn dedicated lightweight background thread
            # Background thread wakes up instantly whenever a smart meter generates a log. 
            # It quietly pulls the log message out of the memory queue and handles the slow, 
            # blocking disk and console writes (stdout, stderr, and RotatingFileHandler). 
            # This keeps the main high-throughput simulation asyncio event loop 100% 
            # free of I/O blocking delays.
            handler.listener.start() 

            # Register an exit hook to flush remaining queue messages on pipeline shutdown
            import atexit
            atexit.register(handler.listener.stop)
