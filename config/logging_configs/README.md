# Explanation of the logging configs

## Logger_congifugration.json

This JSON file configures a non-blocking, asynchronous logging system in Python using a structure typically introduced in Python 3.12+ (via QueueHandler directly in dictConfig). It handles logs by formatting them into three different styles (Simple, Detailed, and JSON) and routes them to different destinations (Standard Output, Standard Error, and a rolling JSON log file) based on their severity.

Broken down by section:

### Formatters (How the logs look)

The configuration defines three ways to style log messages:

- `simple`: Used for normal outputs. It outputs a string showing the severity level, module name, line number, timestamp, and message (e.g., `[INFO | main | L42] 2026-08-31T23:07:00-0400: App started`).

- `detailed`: Used for errors and warnings. It rearranges the metadata to prioritize the exact timestamp and severity level first.

- `json`: Built for machine readability (like log aggregators). It uses a custom Python class (`AppJSONFormatter`) to format the output into structured JSON dictionary objects with keys like timestamp, level, and function.

### Filters (What gets dropped)

- `no_errors`: This instantiates a custom Python filter class called `NonErrorFilter`. Its job is to filter out high-severity messages (like warnings or errors) so they do not clutter the standard output.

### Handlers (Where the logs go)

This section sets up four distinct workers to process log records:

- `stdout`: Sends logs directly to your terminal screen (`sys.stdout`). It uses the simple format and applies the `no_errors` filter to ensure only low-level messages (like DEBUG and INFO) show up here.

- `stderr`: Sends logs to the terminal's standard error stream (`sys.stderr`). It only captures WARNING levels and above, formatting them with the detailed layout.

- `file_json`: Saves everything (DEBUG and up) into a structured file located at `logs/app_log.jsonl`. It uses a rotating mechanism: once the file hits 10,000 bytes, it archives it and rolls over to a new one, keeping a maximum history of 3 backup files.

- `queue_handler`: This is the central manager. Instead of writing to the terminal or file immediately (which can slow down an application), it drops log events into an internal thread-safe queue. It then passes those events down to the `stdout`, `stderr`, and `file_json` targets asynchronously. The `respect_handler_level: true` setting ensures that the individual rules of those sub-handlers (like stderr only taking warnings) are strictly honored.

### Loggers (The user-facing entry points)

- `root`: The catch-all logger for the entire application. It is set to capture everything from the DEBUG level and up, routing all traffic straight into the non-blocking queue_handler.

`app`: A specific named logger wrapper designed for your custom application code, inheriting the same root setup and DEBUG threshold.

---

## mylogger.py

The `mylogger.py` file provides the concrete Python code required to run the custom components referenced in your `logger_configuration.json` file.

Specifically, it defines two critical classes that Python's standard logging library does not provide out of the box: `AppJSONFormatter` and `NonErrorFilter`.

### AppJSONFormatter (The JSON Generator)

This class transforms standard text logs into structured JSON rows (perfect for tools like Datadog, AWS CloudWatch, or Elasticsearch).

- `Key Mapping`: It uses the **fmt_keys** dictionary passed from your JSON config to rename standard logging attributes. For example, it translates Python's internal levelname variable to just level in the final output.

- `Always-Included Fields`: It guarantees that every log row contains a standard ISO-8601 UTC timestamp, the log message, and any relevant error stack traces (`exc_info/stack_info`) if your application crashes.

- `Dynamic Data Capture`: If you pass custom metadata into a log using Python's `extra` parameter—for example: `logger.info("Login", extra={"user_id": 42})`—this formatter automatically loops through the record, filters out standard Python built-ins, and appends `"user_id": 42` directly into the final JSON output.

### NonErrorFilter (The Log Router)

This class prevents logs from overlapping between your terminal's output streams.

- `The Logic`: It inspects the severity level (`levelno`) of every incoming log and returns `True` only if the level is INFO or lower (such as DEBUG and INFO).

- `The Result`: When attached to your `stdout` handler, it explicitly stops WARNING, ERROR, and CRITICAL logs from being printed to `stdout`. This ensures those higher-severity logs are handled exclusively by your `stderr` stream without duplicate entries cluttering your screen.

---

## app_log.jsonl

Structured file located at `logs/app_log.jsonl` that saves everything (DEBUG and up). It uses a rotating mechanism: once the file hits 10,000 bytes, it archives it and rolls over to a new one, keeping a maximum history of 3 backup files.

---
