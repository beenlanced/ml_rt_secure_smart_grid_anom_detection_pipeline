"""
Production-ready test suite for src/simulator.py.
Validates lightweight schema formats, state anomalies, worker deadlines,
queue pressure handling, and async network resiliency using pytest-asyncio.

This test suite utilizes unittest.mock and pytest-asyncio to mock the network
dependencies (aiokafka.AIOKafkaProducer) and global time tracking variables,
allowing quick validation of the simulation matrix without external infrastructure.
"""

import asyncio
import json
import pytest
import time
from typing import Any, Dict, Generator, List, Tuple
from unittest.mock import AsyncMock, patch, MagicMock

# Import the functions and variables from your simulator script
from src.simulator import (
    generate_meter_reading,
    smart_meter_worker,
    kafka_delivery_pipeline,
    time_keeper_worker,
    main,
    TOTAL_METERS,
    KAFKA_TOPIC
)

# ------------------------------------------------------------------------------
# Pytest Fixtures
# Fixtures provide reusable baselines to ensure tests run against isolated,
# repeatable states.
# ------------------------------------------------------------------------------
@pytest.fixture
def mock_kafka_producer() -> Generator[MagicMock, None, None]:
    """
    Patches AIOKafkaProducer to mock lifecycle hooks (.start(), .stop())
    and non-blocking message dispatching (.send()).
    """
    with patch('src.simulator.AIOKafkaProducer') as mock_class:
        mock_instance = MagicMock()
        mock_instance.start = AsyncMock()
        mock_instance.stop = AsyncMock()
        mock_instance.send = AsyncMock()
        mock_class.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_logger() -> Generator[MagicMock, None, None]:
    """Fixture patches the module-level logging system to audit runtime telemetry diagnostic reports."""
    with patch('src.simulator.logger') as mock_log:
        yield mock_log


@pytest.fixture
def telemetry_queue() -> asyncio.Queue[Tuple[str, Dict[str, Any]]]:
    """Fixture to provide a clean, isolated, and empty telemetry queue instance."""
    return asyncio.Queue()


# ------------------------------------------------------------------------------
# 1. Tests for time_keeper_worker (Timeline Architecture)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_time_keeper_worker_updates_global_state() -> None:
    """
    Verifies that the time keeper background task properly accesses time.localtime
    and transforms the values into a normalized float hour value.
    """
    # Mock time.localtime to return a fixed structured time representation (14:30:00)
    mock_time_struct = time.struct_time((2026, 9, 21, 14, 30, 0, 0, 264, 0))
    
    with patch('time.localtime', return_value=mock_time_struct):
        task: asyncio.Task[None] = asyncio.create_task(time_keeper_worker())
        
        # Allow the loop to execute the first tick cycle
        await asyncio.sleep(0.01)
        task.cancel()
        
        # Access module-level shared variable state
        from src import simulator
        # 14 hours + (30 minutes / 60) + (0 seconds / 3600) == 14.5
        assert simulator.CURRENT_LOCAL_HOUR == 14.5


# ------------------------------------------------------------------------------
# 2. Tests for generate_meter_reading (Deterministic Modeling)
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generate_meter_reading_normal_flow() -> None:
    """
    Verifies the production metrics dictionary structure, nested datatypes,
    and US residential baseline metrics under normal, non-anomalous conditions.
    """
    # Force random.random > 0.005 (bypass the 0.5% anomaly) to simulate standard operations
    with patch("random.random", return_value=0.5):
        meter_id: int = 42
        payload: Dict[str, Any] = generate_meter_reading(meter_id)

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
        payload: Dict[str, Any] = generate_meter_reading(meter_id)
        
        assert payload["security_flag"] == 1
        metrics = payload["metrics"]
        
        # Verify physical properties of the simulated grid anomaly
        assert 145.0 <= metrics["voltage_v"] <= 155.0
        assert metrics["current_a"] <= 0.5
        assert metrics["power_factor"] == 0.2

        # Robust Log String Validations
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
# 3. Tests for smart_meter_worker
# ==============================================================================
@pytest.mark.asyncio
async def test_smart_meter_worker_populates_queue(telemetry_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]]) -> None:
    """
    Validates loop indexing logic to ensure concurrent worker segments isolate
    their assigned scale chunks cleanly without cross-over collisions.
    """
    worker_id: int = 0
    meters_per_worker: int = TOTAL_METERS // 100  # 100 meters per worker

    task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id, telemetry_queue))

    # Give the task a quick moment to loop through range 0-99 and yield control via await asyncio.sleep
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
    full_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]] = asyncio.Queue(maxsize=1)
    await full_queue.put(("prefill_id", {}))

    task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id=5, queue=full_queue))
    await asyncio.sleep(0.01)
    task.cancel()

    # The queue must still contain exactly 1 element (did not block or grow past maxsize)
    assert full_queue.qsize() == 1

    # Check that it triggered structural warning drops using modern f-string parsing matching simulator styles
    warning_logged: bool = any(
        "Queue full! Dropping reading for meter 500" in str(call) for call in mock_logger.warning.call_args_list
    )
    assert warning_logged, "Queue saturation load shedding log message was missing."



@pytest.mark.asyncio
async def test_smart_meter_worker_warning_on_delay(
    mock_logger: MagicMock, 
    telemetry_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]]
) -> None:
    """
    Snoozes runtime execution ticks to mimic heavy system event-loop load,
    confirming loop duration calculations emit latency breakdown warning messages.
    """
    worker_id: int = 0

    # Stateful generator that steps forward by 200ms on EVERY single call.
    # This guarantees that 'time.monotonic() - start_time' will always be
    # greater than the 100ms (0.1s) budget limit, regardless of call frequency.
    current_mock_time = 10.0
    def shifting_monotonics() -> float:
        nonlocal current_mock_time
        current_mock_time += 0.2  # 200ms jump per invocation
        return current_mock_time

    with patch('time.monotonic', side_effect=shifting_monotonics):
        task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id=worker_id, queue=telemetry_queue))

        # Give the event loop a brief window to run one iteration and trigger the log
        await asyncio.sleep(0.01)
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify that the warning log was triggered due to loop deadline breach
        mock_logger.warning.assert_called()

        # Grab string payload arguments and check content explicitly
        call_args_string = str(mock_logger.warning.call_args_list)
        assert "processing loop delayed" in call_args_string


# ==============================================================================
# 4. Tests for kafka_delivery_pipeline
# ==============================================================================
@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_processing(
    mock_kafka_producer: MagicMock,
    telemetry_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]]
) -> None:
    """
    Tests pipeline parsing logic, checking that items are continuously
    unloaded from memory structures and fed into Kafka stream layers.
    """
    sample_payload: Dict[str, Any] = {"device_id": "meter_00099", "metrics": {"voltage_v": 120.0}}
    await telemetry_queue.put(("99", sample_payload))

    task: asyncio.Task[None] = asyncio.create_task(kafka_delivery_pipeline(dispatcher_id=1, queue=telemetry_queue))

    # Give the pipeline a quick moment to pull from the queue and send
    await asyncio.sleep(0.01)
    task.cancel()
    
    try:
        await task
    except asyncio.CancelledError:
        pass  # Expected execution exit path

    # Verify connection lifecycles executed completely after the task ended safely
    mock_kafka_producer.start.assert_called_once()
    mock_kafka_producer.stop.assert_called_once()

    # Verify send was invoked with native pass-through structures
    mock_kafka_producer.send.assert_called_with(
        topic=KAFKA_TOPIC,
        value=sample_payload,
        key="99"
    )


@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_error_resilience(
    mock_kafka_producer: MagicMock, 
    mock_logger: MagicMock, 
    telemetry_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]]
) -> None:
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
    
    try:
        await task
    except asyncio.CancelledError:
        pass

    # Check that error handler captured the anomaly with custom tracking indicators and logged it
    mock_logger.error.assert_called_once()
    
    # Audit log string tracking indicators matching the updated simulator.py error message signature
    call_args_string = str(mock_logger.error.call_args_list)
    assert "pipeline error" in call_args_string
    assert "🚨 Dispatcher #2 encountered a pipeline error" in call_args_string
    
    # Confirm resource cleanup was strictly maintained through the finally block
    mock_kafka_producer.stop.assert_called_once()


@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_throughput_logging(
    mock_logger: MagicMock, 
    mock_kafka_producer: MagicMock, 
    telemetry_queue: asyncio.Queue[Tuple[str, Dict[str, Any]]]
) -> None:
    """Mocks time references to mimic a 5-second interval passage, verifying performance metrics logging."""
    await telemetry_queue.put(("1", {"device_id": "meter_00001"}))

    call_count = 0

    def dynamic_clock() -> float:
        nonlocal call_count
        call_count += 1
        if call_count <= 3:
            return 10.0
        # Subsequent iterations leap forward to 16.0 (6 seconds elapsed) to trigger metrics logs
        return 16.0
    
    with patch('time.monotonic', side_effect=dynamic_clock):
        task: asyncio.Task[None] = asyncio.create_task(kafka_delivery_pipeline(dispatcher_id=3, queue=telemetry_queue))

        # Give the loop an execution window to step forward and compute the logs
        await asyncio.sleep(0.01)
        task.cancel()
        
        try:
            await task
        except asyncio.CancelledError:
            pass

        # Audit logger info history records for status reports matching custom f-string patterns
        any_throughput_logged: bool = any(
            "Throughput Status: Sent 1 records" in str(call) for call in mock_logger.info.call_args_list
        )
        assert any_throughput_logged, "Pipeline performance metric report was missing or formatted incorrectly."


# ==============================================================================
# 5. Integration Tests for Main Application Orchestration Loop
# ==============================================================================
@pytest.mark.asyncio
async def test_main_orchestration_lifecycle_shutdown(
    mock_kafka_producer: MagicMock, 
    mock_logger: MagicMock
) -> None:
    """
    Verifies complete orchestration startup execution matrix paths and validates
    that operator cancellation signals cause a safe, graceful teardown sequence.
    """
    # Override scaling limits within the patch statement context to minimize event loop overhead
    with patch('src.simulator.NUM_WORKERS', 2), patch('src.simulator.NUM_DISPATCHERS', 1):
        main_task: asyncio.Task[None] = asyncio.create_task(main())
        
        # Allow the matrix clusters to spin up, allocate queues, and start processing updates
        await asyncio.sleep(0.05)
        
        # Simulate operator termination interrupt signal
        main_task.cancel()
        
        try:
            await main_task
        except asyncio.CancelledError:
            pass  # Expected execution cleanup path
            
        # Ensure log statements explicitly audited the shutdown workflow lifecycle steps
        log_output = str(mock_logger.info.call_args_list)
        assert "Stopping all background simulation and infrastructure tasks..." in log_output
        assert "Grid simulator pipeline shutdown complete." in log_output
