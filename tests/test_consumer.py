import json
import pytest
from datetime import datetime, timezone
from typing import Any, Generator, List, Tuple
from unittest.mock import ANY, MagicMock, patch

import psycopg2
from confluent_kafka import KafkaError, Consumer, Message
from psycopg2.extensions import connection as PgConnection
from psycopg2.extensions import cursor as PgCursor 

### Import the targeted consumer logic and configuration constraints
from src.consumer import (
    create_db_connection,
    insert_batch_with_retry,
    main,
    DB_DSN,
    BATCH_SIZE,
    MAX_BATCH_AGE_SEC
) 

### Define Type Alias for database row structures to ensure parity with production code
type TelemetryRow = Tuple[str, int, float, float, float, float, int] 

### ------------------------------------------------------------------------------
### 1. Pytest Fixtures (Mock Infrastructure Isolation)
### ------------------------------------------------------------------------------

@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Patches the module-level logger to inspect system telemetry diagnostics."""
    with patch('src.consumer.logger') as mock_log:
        yield mock_log

@pytest.fixture
def mock_db_connection() -> MagicMock:
    """
    Constructs a completely mocked psycopg2 connection.
    Wires up inner execution frameworks to safely yield an active mock cursor.
    """
    mock_conn: MagicMock = MagicMock(spec=PgConnection)
    mock_cursor: MagicMock = MagicMock(spec=PgCursor) 

    # Fixed syntax: Correctly accessing the context manager hook
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn

@pytest.fixture
def mock_kafka_consumer() -> Generator[MagicMock, None, None]:
    """Patches Confluent Kafka Consumer class to intercept polling loop lifecycle hooks."""
    with patch('src.consumer.Consumer') as mock_class:
        mock_instance: MagicMock = MagicMock(spec=Consumer)
        mock_class.return_value = mock_instance
        yield mock_instance

@pytest.fixture
def sample_kafka_message() -> MagicMock:
    """Generates a factory-mocked Kafka Message instance loaded with safe residential baseline data."""
    mock_msg: MagicMock = MagicMock(spec=Message)
    mock_msg.error.return_value = None

    payload: dict[str, Any] = {
        "timestamp": 1710000000.0,
        "device_id": "meter_00042",
        "metrics": {
            "voltage_v": 120.0,
            "current_a": 1.5,
            "power_kw": 0.18,
            "power_factor": 0.95
        },
        "security_flag": 0
    }

    mock_msg.value.return_value = json.dumps(payload).encode('utf-8')
    return mock_msg

### ------------------------------------------------------------------------------
### 2. Infrastructure & Connection Layer Unit Tests
### ------------------------------------------------------------------------------

def test_create_db_connection_success(mock_db_connection: MagicMock) -> None:
    """Verifies DSN passing and valid connection return value under normal network conditions."""
    with patch('psycopg2.connect', return_value=mock_db_connection) as mock_connect:
        conn: PgConnection | None = create_db_connection()
        assert conn == mock_db_connection
        mock_connect.assert_called_once_with(DB_DSN)

def test_create_db_connection_failure(mock_logger: MagicMock) -> None:
    """Ensures operational driver exceptions return None safely instead of throwing unhandled failures."""
    with patch('psycopg2.connect', side_effect=psycopg2.OperationalError("Connection timed out")):
        conn: PgConnection | None = create_db_connection()
        assert conn is None
        mock_logger.error.assert_called_once()

### ------------------------------------------------------------------------------
### 3. Database Write Logic & Retry Engine Unit Tests
### ------------------------------------------------------------------------------

def test_insert_batch_with_retry_success(mock_db_connection: MagicMock, mock_logger: MagicMock) -> None:
    """Validates standard workflow: micro-batch arrays are compiled, executed bulk, and committed."""
    sample_batch: List[TelemetryRow] = [("2026-09-19T23:34:00Z", "meter_00001", 120.0, 1.0, 0.12, 0.95, 0)] 

    with patch('src.consumer.execute_values') as mock_execute_values:
        success: bool = insert_batch_with_retry(mock_db_connection, sample_batch)
        assert success is True
        mock_execute_values.assert_called_once()
        mock_db_connection.commit.assert_called_once()
        mock_logger.debug.assert_called_once()

def test_insert_batch_with_retry_network_fault_resilience(mock_db_connection: MagicMock, mock_logger: MagicMock) -> None:
    """Ensures intermediate database network anomalies trigger rollback sequences and backoff delays."""
    sample_batch: List[TelemetryRow] = [("2026-09-19T23:34:00Z", "meter_00001", 120.0, 1.0, 0.12, 0.95, 0)]

    # Added explicit line continuations (\) to prevent indentation syntax errors
    with patch('src.consumer.execute_values', side_effect=[psycopg2.OperationalError("Packet drop"), None]) as mock_exec, \
         patch('time.sleep') as mock_sleep:
        success: bool = insert_batch_with_retry(mock_db_connection, sample_batch, max_retries=2)

        assert success is True
        assert mock_exec.call_count == 2
        mock_db_connection.rollback.assert_called_once()
        mock_sleep.assert_called_once_with(1.0)  # Validates initial exponential backoff delay calculation
        mock_logger.warning.assert_called_once()

def test_insert_batch_with_retry_fatal_exception(mock_db_connection: MagicMock, mock_logger: MagicMock) -> None:
    """Confirms systemic violations (e.g. data constraint errors) break the loop instantly instead of retrying."""
    sample_batch: List[TelemetryRow] = [("2026-09-19T23:34:00Z", "meter_00001", -500.0, 1.0, 0.12, 0.95, 0)]

    with patch('src.consumer.execute_values', side_effect=ValueError("Data type mismatch")):
        success: bool = insert_batch_with_retry(mock_db_connection, sample_batch)
        assert success is False
        mock_db_connection.rollback.assert_called_once()
        mock_logger.error.assert_called_once()

### ------------------------------------------------------------------------------
### 4. Stream Processing Loop & Micro-Batching Integration Tests
### ------------------------------------------------------------------------------

def test_main_database_init_failure(mock_logger: MagicMock) -> None:
    """Verifies execution loop halts completely if the target analytics engine cannot clear safety barriers."""
    with patch('src.consumer.create_db_connection', return_value=None):
        main()
        mock_logger.critical.assert_called_with("Fatal: Database connection returned None. Exiting...")

def test_main_processing_batch_flushing_on_size(
    mock_kafka_consumer: MagicMock,
    mock_db_connection: MagicMock,
    sample_kafka_message: MagicMock
) -> None:
    """Ensures micro-batches automatically force data-flushes down to the hypertable when BATCH_SIZE is crossed."""

    with patch('src.consumer.create_db_connection', return_value=mock_db_connection),\
         patch('src.consumer.insert_batch_with_retry', return_value=True) as mock_insert,\
         patch('src.consumer.BATCH_SIZE', 2):

        mock_kafka_consumer.poll.return_value = sample_kafka_message
        mock_kafka_consumer.commit.side_effect = [None, KeyboardInterrupt()]

        main()

        assert mock_insert.call_count >= 1
        mock_kafka_consumer.commit.assert_called_with(asynchronous=False)

def test_main_processing_batch_flushing_on_temporal_age(
    mock_kafka_consumer: MagicMock,
    mock_db_connection: MagicMock,
    sample_kafka_message: MagicMock
) -> None:
    """Verifies items hanging in internal buffers are safely written if MAX_BATCH_AGE_SEC matches, preventing data stall."""

    with patch('src.consumer.create_db_connection', return_value=mock_db_connection),\
         patch('src.consumer.insert_batch_with_retry', return_value=True) as mock_insert:

        # Resolved missing timeline sequences 
        clock_ticks: List[float] = [100.0, 100.0 + MAX_BATCH_AGE_SEC + 1.0]

        def dynamic_clock() -> float:
            return clock_ticks.pop(0) if clock_ticks else 200.0

        mock_kafka_consumer.poll.side_effect = [sample_kafka_message, None, KeyboardInterrupt()]

        with patch('time.time', side_effect=dynamic_clock):
            main()

        # Verify that mock_insert was called with the database connection as the
        # first argument, and any list as the second argument.
        mock_insert.assert_any_call(mock_db_connection, ANY)

def test_main_voltage_anomaly_alerting(
    mock_kafka_consumer: MagicMock,
    mock_db_connection: MagicMock,
    mock_logger: MagicMock
) -> None:
    """Audits processing pipeline filters, tracking that high-surge grid values trigger warning alerts."""
    mock_msg: MagicMock = MagicMock(spec=Message)
    mock_msg.error.return_value = None

    high_voltage_payload: dict[str, Any] = {
        "timestamp": 1710000000.0,
        "device_id": "meter_critical_node_88",
        "metrics": {"voltage_v": 145.2, "current_a": 0.5, "power_kw": 0.07, "power_factor": 0.33},
        "security_flag": 1
    }
    mock_msg.value.return_value = json.dumps(high_voltage_payload).encode('utf-8')
    mock_kafka_consumer.poll.side_effect = [mock_msg, KeyboardInterrupt()]

    # Cleaned and completed the trailing execution wrap-around
    with patch('src.consumer.create_db_connection', return_value=mock_db_connection),\
         patch('src.consumer.insert_batch_with_retry', return_value=True):
        main()
        
    # Verify warning thresholds correctly emitted logger events
    mock_logger.warning.assert_called()
