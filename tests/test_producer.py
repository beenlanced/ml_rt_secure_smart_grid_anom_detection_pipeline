# Because your code relies on aiokafka.AIOKafkaProducer (which requires a running Kafka broker by default), 
# these tests use unittest.mock and pytest-asyncio to mock the network dependencies. 
# This allows you to test the logic, queues, and throughput logging instantly without 
# setting up a real Kafka or Redpanda instance.

import asyncio
import json
import pytest
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
# ------------------------------------------------------------------------------
@pytest.fixture
def mock_kafka_producer() -> Generator[MagicMock, None, None]:
    """
    Fixture to patch AIOKafkaProducer and configure standard AsyncMocks 
    for lifecycle and delivery methods (.start(), .stop(), .send()).
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
    """Fixture to patch the module-level logger for asserting log behavior."""
    with patch('src.producer.logger') as mock_log:
        yield mock_log


@pytest.fixture
def telemetry_queue() -> asyncio.Queue[tuple[str, dict[str, Any]]]:
    """Fixture to provide a clean, empty telemetry queue instance."""
    return asyncio.Queue()


# ------------------------------------------------------------------------------
# 1. Tests for generate_meter_reading
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_generate_meter_reading_structure() -> None:
    """
    Verifies that the generated payload contains the correct dictionary structure and data types.
    Assures that the generated keys match structural needs and ranges remain bounded.
    """
    meter_id: int = 42
    payload: dict[str, Any] = await generate_meter_reading(meter_id)
    
    assert isinstance(payload, dict)
    assert payload["m_id"] == meter_id
    assert isinstance(payload["v"], float)
    assert 115.0 <= payload["v"] <= 125.0
    assert isinstance(payload["c"], float)
    assert 5.0 <= payload["c"] <= 15.0
    assert isinstance(payload["t"], int)


# ------------------------------------------------------------------------------
# 2. Tests for smart_meter_worker
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_smart_meter_worker_populates_queue(telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Verifies that a worker successfully puts the expected number of meter readings into the queue.
    Verifies that your loop chunks the ranges correctly (0-99 for worker 0) and pushes items cleanly
    into the queue.
    """
    worker_id: int = 0
    meters_per_worker: int = TOTAL_METERS // 100  # 100 meters per worker based on 10,000 total
    
    # Run the worker, but cancel it immediately after one loop execution to prevent infinite loop
    task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id, telemetry_queue))
    
    # Allow the loop to run once and hit the internal sleep
    await asyncio.sleep(0.05)
    task.cancel()
    
    # Check that the queue received the correct number of items for this worker segment
    assert telemetry_queue.qsize() == meters_per_worker
    
    # Verify the bounds of the first item in the queue (Worker 0 handles meters 0-99)
    key, payload = await telemetry_queue.get()
    assert key == "0"
    assert payload["m_id"] == 0


@pytest.mark.asyncio
async def test_smart_meter_worker_warning_on_delay(mock_logger: MagicMock, telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Simulates a slow generator to verify the worker triggers a warning log when breaching the 100ms window.
    Artificially slows down the payload speed to assert that your time budget boundary alert works flawlessly.
    """
    # Mock the generator to simulate a delay larger than INTERVAL_SEC (0.1s)
    async def slow_generator(meter_id: int) -> dict[str, Any]:
        await asyncio.sleep(0.002) # Tiny sleep per meter to cross the 100ms limit collectively
        return {"m_id": meter_id, "v": 120.0, "c": 10.0, "t": 12345}

    with patch('src.producer.generate_meter_reading', side_effect=slow_generator):
        task: asyncio.Task[None] = asyncio.create_task(smart_meter_worker(worker_id=0, queue=telemetry_queue))
        await asyncio.sleep(0.25)  # Let it complete at least one delayed cycle
        task.cancel()
        
        # Verify that the warning log was triggered due to loop deadline breach
        mock_logger.warning.assert_called()
        assert "processing loop delayed" in mock_logger.warning.call_args


# ------------------------------------------------------------------------------
# 3. Tests for kafka_delivery_pipeline
# ------------------------------------------------------------------------------
@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_processing(mock_kafka_producer: MagicMock, telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Verifies that the pipeline starts the producer, drains the queue, and serializes payloads correctly.
    Validates the end-to-end payload data conversion pipeline (JSON dict → UTF-8 Encoded String → Bytes)
    and verifies that Kafka lifecycle commands (.start(), .send(), .stop()) execute smoothly.
    """
    sample_payload: dict[str, Any] = {"m_id": 99, "v": 120.0, "c": 10.0, "t": 1600000000}
    await telemetry_queue.put(("99", sample_payload))

    # Run pipeline dispatcher
    task: asyncio.Task[None] = asyncio.create_task(kafka_delivery_pipeline(dispatcher_id=1, queue=telemetry_queue))
    
    # Allow the event loop to process the item in the queue
    await asyncio.sleep(0.05)
    task.cancel()

    # Assert lifecycle methods were executed via the fixture instance
    mock_kafka_producer.start.assert_called_once()
    mock_kafka_producer.stop.assert_called_once()
    
    # Assert network delivery arguments match expected data transformation
    expected_bytes: bytes = json.dumps(sample_payload).encode('utf-8')
    mock_kafka_producer.send.assert_called_with(
        topic=KAFKA_TOPIC,
        value=expected_bytes,
        key=b"99"
    )


@pytest.mark.asyncio
async def test_kafka_delivery_pipeline_throughput_logging(mock_logger: MagicMock, mock_kafka_producer: MagicMock, telemetry_queue: asyncio.Queue[tuple[str, dict[str, Any]]]) -> None:
    """
    Verifies that throughput stats are logged if the 5-second interval condition is reached.
    Mocks the monotonic baseline clock to trick the engine into satisfying the 5.0 second 
    threshold rule, validating runtime metrics code stability.
    """
    await telemetry_queue.put(("1", {"m_id": 1}))

    # Mock time.monotonic to simulate 6 seconds passing on the second loop execution
    with patch('time.monotonic', side_effect=[10.0, 10.0, 10.0, 16.0, 16.0]):
        task: asyncio.Task[None] = asyncio.create_task(kafka_delivery_pipeline(dispatcher_id=1, queue=telemetry_queue))
        await asyncio.sleep(0.05)
        task.cancel()

        # Check if info log reporting metrics was called
        any_throughput_logged: bool = any(
            "Throughput Status" in call for call in mock_logger.info.call_args_list
        )
        assert any_throughput_logged, "Throughput metric report was not logged."
