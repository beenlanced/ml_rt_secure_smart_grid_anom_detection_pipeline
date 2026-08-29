import asyncio
import json
import logging
from typing import Any, Dict, Generator, List, Union
from unittest.mock import MagicMock, AsyncMock, patch

import pytest
from _pytest.logging import LogCaptureFixture
from confluent_kafka import KafkaError, Message, Producer

#import simulator  # Import your code module here
from src.simulator import (
    delivery_report,
    kafka_flush_worker,
    simulate_smart_meter,
    main
)

# --- FIXTURES ---

@pytest.fixture
def mock_kafka_message() -> MagicMock:
    """Generates a mock successful Kafka message."""
    msg: MagicMock = MagicMock(spec=Message)
    msg.topic.return_value = "smartgrid-telemetry"
    msg.partition.return_value = 0
    return msg

@pytest.fixture
def mock_kafka_error() -> MagicMock:
    """Generates a mock generic Kafka error."""
    err: MagicMock = MagicMock(spec=KafkaError)
    err.code.return_value = 999
    err.__str__.return_value = "Generic Kafka Error"
    return err

@pytest.fixture
def mock_producer() -> MagicMock:
    """Creates a mock Kafka Producer client."""
    producer: MagicMock = MagicMock(spec=Producer)
    return producer


# --- TEST CASES ---

class TestKafkaDeliveryCallback:
    """Tests the delivery_report network error and success paths."""

    def test_delivery_success(self, mock_kafka_message: MagicMock, caplog: LogCaptureFixture) -> None:
        """Ensures that a successful message delivery doesn't log errors."""
        with caplog.at_level(logging.ERROR):
            delivery_report(err=None, msg=mock_kafka_message)
        
        # Should not log any errors or criticals
        assert len(caplog.records) == 0

    @pytest.mark.parametrize("error_code, expected_log", [
        (KafkaError._TRANSPORT, "Network transport failure"),
        (KafkaError._TIMED_OUT, "Network timeout"),
        (KafkaError._ALL_BROKERS_DOWN, "Critical: All Kafka brokers are down"),
        (999, "Message delivery failed with error code 999"),
    ])
    def test_delivery_failures(
        self, 
        error_code: int, 
        expected_log: str, 
        mock_kafka_message: MagicMock, 
        caplog: LogCaptureFixture
    ) -> None:
        """Tests individual Kafka error code routing and logging hooks."""
        err: MagicMock = MagicMock(spec=KafkaError)
        err.code.return_value = error_code
        err.__str__.return_value = "Mocked native exception detail."

        with caplog.at_level(logging.ERROR):
            delivery_report(err=err, msg=mock_kafka_message)

        assert len(caplog.records) == 1
        assert expected_log in caplog.text


class TestKafkaFlushWorker:
    """Tests the background flusher network loop."""

    @pytest.mark.asyncio
    async def test_flush_worker_polls_and_yields(self, mock_producer: MagicMock) -> None:
        """Verifies that the flush worker polls the producer interface and yields control."""
        # Wrap the worker loop inside a task timeout window to break out of its infinite while loop
        task: asyncio.Task[None] = asyncio.create_task(kafka_flush_worker(mock_producer))
        
        # Allow it to loop a few times
        await asyncio.sleep(0.05)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify that poll was executed to sweep callback queues
        assert mock_producer.poll.called
        assert mock_producer.poll.call_args == 0.01  # Verifies 10ms poll time


class TestSmartMeterSimulation:
    """Tests individual virtual IoT device telemetry behavior."""

    @pytest.mark.asyncio
    @patch('simulator.random.random')
    async def test_simulate_smart_meter_normal_payload(
        self, 
        mock_random: MagicMock, 
        mock_producer: MagicMock
    ) -> None:
        """Validates that a normal telemetry iteration builds correct JSON format fields."""
        # Forces normal telemetry path (skips the 0.5% anomaly chance)
        mock_random.return_value = 0.5 
        
        task: asyncio.Task[None] = asyncio.create_task(simulate_smart_meter(device_id=7, producer=mock_producer))
        await asyncio.sleep(0.02)  # Allow first execution iteration
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify the message structural keys sent to Kafka
        mock_producer.produce.assert_called()
        called_kwargs: Dict[str, Any] = mock_producer.produce.call_args.kwargs
        
        assert called_kwargs['topic'] == 'smartgrid-telemetry'
        assert called_kwargs['key'] == 'meter_00007'
        
        # Validate binary data serialization payload
        payload: Dict[str, Any] = json.loads(called_kwargs['value'].decode('utf-8'))
        assert payload['device_id'] == 'meter_00007'
        assert payload['security_flag'] == 0
        assert 'voltage_v' in payload['metrics']
        assert 'current_a' in payload['metrics']

    @pytest.mark.asyncio
    @patch('smartgrid_simulator.random.random')
    async def test_simulate_smart_meter_anomaly_injection(
        self, 
        mock_random: MagicMock, 
        mock_producer: MagicMock, 
        caplog: LogCaptureFixture
    ) -> None:
        """Forces an anomaly calculation to check if cyber warnings trigger and scale metrics."""
        # Forces anomaly execution path (< 0.005 chance)
        mock_random.return_value = 0.001 
        
        task: asyncio.Task[None] = asyncio.create_task(simulate_smart_meter(device_id=42, producer=mock_producer))
        await asyncio.sleep(0.02)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        # Verify warnings are triggered on the console
        assert "Cyber-anomaly injected!" in caplog.text
        
        called_kwargs: Dict[str, Any] = mock_producer.produce.call_args.kwargs
        payload: Dict[str, Any] = json.loads(called_kwargs['value'].decode('utf-8'))
        
        assert payload['security_flag'] == 1
        assert 140.0 <= payload['metrics']['voltage_v'] <= 160.0

    @pytest.mark.asyncio
    async def test_simulate_smart_meter_buffer_full_retry(
        self, 
        mock_producer: MagicMock, 
        caplog: LogCaptureFixture
    ) -> None:
        """Ensures that localized BufferError constraints trigger async sleep backoff flags."""
        # Side effect triggers a BufferError on first call, then successfully returns on second
        mock_producer.produce.side_effect = [BufferError("Local queue full"), None]

        task: asyncio.Task[None] = asyncio.create_task(simulate_smart_meter(device_id=1, producer=mock_producer))
        await asyncio.sleep(0.08)  # Enough window to hit BufferError and complete retry cycle (50ms)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass

        assert "Local producer buffer full. Retrying in 50ms..." in caplog.text
        assert mock_producer.produce.call_count == 2


class TestMainOrchestration:
    """Verifies complete application orchestration layer."""

    @pytest.mark.asyncio
    @patch('simulator.Producer')
    @patch('simulator.NUM_DEVICES', 2)  # Reduce parallel count down to 2 for quick unit test
    async def test_main_pipeline_lifecycle(self, mock_producer_class: MagicMock) -> None:
        """Ensures application safely instantiates, cancels tasks, and drains network queues."""
        mock_prod_instance: MagicMock = MagicMock(spec=Producer)
        mock_prod_instance.flush.return_value = 0
        mock_producer_class.return_value = mock_prod_instance

        # Create task execution context for main pipeline function
        main_task: asyncio.Task[None] = asyncio.create_task(main())
        await asyncio.sleep(0.05)  # Let devices orchestrate and run
        
        # Simulate an operator interrupting the service via Ctrl+C / Cancel
        main_task.cancel()

        try:
            await main_task
        except asyncio.CancelledError:
            pass

        # Verify safe shutdown procedures are followed
        mock_prod_instance.flush.assert_called_once_with(timeout=5)
