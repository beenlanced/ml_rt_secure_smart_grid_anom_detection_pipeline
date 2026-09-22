"""
Production-ready test suite for src/producer.py.
Validates lightweight schema formats, state anomalies, worker deadlines, 
queue pressure handling, and async network resiliency using pytest-asyncio.

Because your code relies on aiokafka.AIOKafkaProducer (which requires a running Kafka broker by default), 
these tests use unittest.mock and pytest-asyncio to mock the network dependencies. 
This allows you to test the logic, queues, and throughput logging instantly without 
setting up a real Kafka or Redpanda instance.
"""

import asyncio
import json
import pytest
import time
from typing import Any, Generator
from unittest.mock import AsyncMock, patch, MagicMock

# Import the functions and variables from your script
from src.producer import (
    generate_meter_reading,
    smart_meter_worker,
    kafka_delivery_pipeline,
    TOTAL_METERS,
    KAFKA_TOPIC
)

# ------------------------------------------------------------------------------
# Pytest Fixtures
# fixtures are reusable functions designed to set up a fixed baseline such 
# as test data, database connections, mock clients, or environment 
# configuration so that your tests run against a known, repeatable state
# ------------------------------------------------------------------------------
@pytest.fixture
def mock_kafka_producer() -> Generator[MagicMock, None, None]:
    """
    Patches AIOKafkaProducer to mock lifecycle hooks (.start(), .stop())
    and non-blocking message dispatching (.send()).
    """
    with patch('src.producer.AIOKafkaProducer') as mock_class:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.stop = AsyncMock()
        mock_instance.send = AsyncMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Fixture patches the module-level logging system to audit runtime telemetry diagnostic reports."""
    with patch('src.producer.logger') as mock_log:
        yield mock_log


@pytest.fixture
def telemetry_queue() -> asyncio.Queue[tuple[str, dict[str, Any]]]:
    """Fixture to provide a clean, isolated, and empty telemetry queue instance."""
    return asyncio.Queue()


# ------------------------------------------------------------------------------
# 1. Tests for generate_meter_reading (Deterministic Modeling)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio #decorator tells pytest framework that a test f() is an async coroutine and is executed inside an asyncio event loop
async def test_generate_meter_reading_normal_flow() -> None:
    """
    Verifies the production metrics dictionary structure, nested datatypes,
    and US residential baseline metrics under normal, non-anomalous conditions.
    """
    # Force random.random > 0.005 (bypass the 0.5% anomaly) to simulate standard operations
    # patch is a function from the unittest.mock library used to temporarily replace an object or function with a mock object
    with patch("random.random", return_value=0.5):
        meter_id: int = 42
        payload: dict[str, Any] = generate_meter_reading(meter_id)

        # Structure validations
        assert isinstance(payload, dict)
        assert "timestamp" in payload
        assert isinstance(payload["timestamp"], float)
        assert payload["device_id"] == "meter_00042"
        assert payload["security_flag"] == 0

        # Metrics block validations
        metrics = payload["metrics"]
        assert isinstance(metrics, dict)
        assert isinstance(metrics["voltage_v"], float)
        assert isinstance(metrics["current_a"], float)
        assert isinstance(metrics["power_kw"], float)
        assert isinstance(metrics["power_factor"], float)

        # Grid range limits
        assert metrics["current_a"] >= 0.1
        assert 0.85 <= metrics["power_factor"] <= 0.97


@pytest.mark.asyncio
async def test_generate_meter_reading_cyber_anomaly_injection(mock_logger: MagicMock) -> None:
    """
    Forces random.random below the 0.5% threshold to ensure malicious grid attacks/surges
    trip security flags, force zero-phase alignment, and log warnings correctly.
    """
    # Force random.random < 0.005 to trigger cyber-anomaly path
    with patch('random.random', return_value=0.001):
        meter_id: int = 99
        payload: dict[str, Any] = generate_meter_reading(meter_id)
        
        assert payload["security_flag"] == 1
        metrics = payload["metrics"]
        
        # Verify physical properties of the simulated grid anomaly
        assert 145.0 <= metrics["voltage_v"] <= 155.0
        assert metrics["current_a"] <= 0.5
        assert metrics["power_factor"] == 0.2

        # Robust Log String Validations
        # Ensure the logger's warning method was invoked
        mock_logger.warning.assert_called_once()
        
        # Extract the positional arguments from the call tuple (args, kwargs)
        call_args, _ = mock_logger.warning.call_args
        actual_log_message = call_args[0] if call_args else ""
        
        # Audit critical identifiers and text signatures within the string
        assert "Cyber-anomaly injected!" in actual_log_message
        assert "[meter_00099]" in actual_log_message
        assert f"V={metrics['voltage_v']:.2f}V" in actual_log_message
        assert f"A={metrics['current_a']:.2f}A" in actual_log_message


# ==============================================================================
# 2. Tests for smart_meter_worker
# ==============================================================================
@pytest.mark.asyncio
async def test_smart_meter_worker_populates_queue(telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Validates loop indexing logic to ensure concurrent worker segments isolate
    their assigned scale chunks cleanly without cross-over collisions.
    """
    worker_id: int = 0
    meters_per_worker: int = TOTAL_METERS // 100  # 100 meters per worker

    task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id, telemetry_queue))

    # Give the task a quick moment to loop through range 0-99 and enter sleep state
    await asyncio.sleep(0.01)
    task.cancel()

    assert telemetry_queue.qsize() == meters_per_worker

    # Pull first element to check bounds alignment
    key, payload = await telemetry_queue.get()
    assert key == "0"
    assert payload["device_id"] == "meter_00000"


@pytest.mark.asyncio
async def test_smart_meter_worker_handles_queue_full(mock_logger: MagicMock) -> None:
    """
    Ensures that when network delivery dispatchers slow down or the queue hits limits,
    the worker sheds load safely via put_nowait instead of halting execution.
    """
    # Construct a completely filled, tight-bounded storage queue
    full_queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=1)
    await full_queue.put(("prefill_id", {}))

    task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id=5, queue=full_queue))
    await asyncio.sleep(0.01)
    task.cancel()

    # The queue must still contain exactly 1 element (did not block or grow past maxsize)
    assert full_queue.qsize() == 1
    # Check that it triggered structural warning drops
    mock_logger.warning.assert_any_call("Queue full! Dropping reading for meter %d", 500)


@pytest.mark.asyncio
async def test_smart_meter_worker_warning_on_delay(mock_logger: MagicMock, telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Snoozes runtime execution ticks to mimic heavy system event-loop load,
    confirming loop duration calculations emit latency breakdown warning messages.
    """
    worker_id: int = 0

    # Inject artificial sleep into telemetry generator to blow the 100ms deadline budget
    # Provide an ultra-lightweight slow generator that actually gets wired up
    # A tiny real sleep per meter across 100 meters easily breaches the 100ms (0.1s) budget
    def slow_generator(meter_id: int) -> dict[str, Any]:
        time.sleep(0.0015)
        return {"device_id": f"meter_{meter_id:05d}", "metrics": {}}

     # Wire the slow generator directly to the function patch context
    # Mock time.monotonic to simulate 0.0s starting, then instantly jumping to 0.2s
    # when the worker checks elapsed duration, forcing a deadline breach without using real delays.
    with patch ('src.producer.generate_meter_reading', side_effect=slow_generator):
        task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id=worker_id, queue=telemetry_queue))

        # Give it slightly more time to cleanly finish the 100 slow iterations
        await asyncio.sleep(0.2)  
        task.cancel()

        # Verify that the warning log was triggered due to loop deadline breach
        mock_logger.warning.assert_called()

        # Grab string payload arguments and check content explicitly
        call_args_string = str(mock_logger.warning.call_args_list)
        assert "processing loop delayed" in call_args_string


# ==============================================================================
# 3. Tests for kafka_delivery_pipeline
# ==============================================================================
@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_processing(mock_kafka_producer: MagicMock, telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Tests pipeline parsing logic, checking that items are continuously
    unloaded from memory structures and fed into Kafka stream layers.
    """
    sample_payload: dict[str, Any] = {"device_id": "meter_00099", "metrics": {"voltage_v": 120.0}}
    await telemetry_queue.put(("99", sample_payload))

    task: asyncio.Task[None] = asyncio.create_task(kafka_delivery_pipeline(dispatcher_id=1, queue=telemetry_queue))

    # Give the pipeline a quick moment to pull from the queue and send
    await asyncio.sleep(0.01)

    # Cancel the task and await its completion within a try/except block
    # to let the event loop process the finally cleanup step entirely
    # await task after task.cancel() instructs the test runner
    # to pause and let the background worker task execute its remaining lifecycles
    # until it dies completely
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass  # Expected execution exit path

    # Verify connection lifecycles executed completely after the task ended safely
    mock_kafka_producer.start.assert_called_once()
    mock_kafka_producer.stop.assert_called_once()

    # Verify send was invoked with native pass-through structures
    # (Since value/key serialization parameters are handled internally by AIOKafkaProducer initialization)
    mock_kafka_producer.send.assert_called_with(
        topic=KAFKA_TOPIC,
        value=sample_payload,
        key="99"
    )


@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_error_resilience(mock_kafka_producer: MagicMock, mock_logger: MagicMock, telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Simulates severe broker connectivity faults to verify that the pipeline handles crashes
    gracefully, records structural stack traces, and tears down lingering connection tasks.
    """
    await telemetry_queue.put(("10", {"device_id": "meter_00010"}))
    
    # Simulate a network crash during message delivery
    mock_kafka_producer.send.side_effect = RuntimeError("Broker network partition failure")
    
    task = asyncio.create_task(kafka_delivery_pipeline(dispatcher_id=2, queue=telemetry_queue))
    await asyncio.sleep(0.01)
    task.cancel()
    
    # Check that error handler captured the anomaly and logged it
    mock_logger.error.assert_called_once()
    # Confirm resource cleanup was strictly maintained through the finally block
    mock_kafka_producer.stop.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_throughput_logging(mock_logger: MagicMock, mock_kafka_producer: MagicMock, telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """Mocks time references to mimic a 5-second interval passage, verifying performance metrics logging."""
    await telemetry_queue.put(("1", {"device_id": "meter_00001"}))

    # A stateful counter to step forward dynamically without running out of values
    call_count = 0

    # Use a custom dynamic function for side_effect. Function
    # tracks state changes to step forward past the 5-second interval once
    # and then continuously defaults to returning a stable value if called repeatedly
    def dynamic_clock() -> float:
        nonlocal call_count
        call_count += 1
        # First few configuration checks get baseline 10.0
        if call_count <= 3:
            return 10.0
        # Subsequent iterations leap forward to 16.0 (6 seconds elapsed) to trigger metrics
        return 16.0
    
    with patch('time.monotonic', side_effect=dynamic_clock):
        task: asyncio.Task[None] = asyncio.create_task(kafka_delivery_pipeline(dispatcher_id=3, queue=telemetry_queue))

        # Give the loop an execution window to step forward and compute the logs
        await asyncio.sleep(0.01)

        # Cancel the task and await its completion within a try/except block
        # to let the event loop process the finally cleanup step entirely
        # await task after task.cancel() instructs the test runner
        # to pause and let the background worker task execute its remaining lifecycles
        # until it dies completely
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Audit logger info history records for status reports
        any_throughput_logged: bool = any(
            "Throughput Status" in str(call) for call in mock_logger.info.call_args_list
        )
        assert any_throughput_logged, "Pipeline performance metric report was missing."