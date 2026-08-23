from typing import Optional
from unittest.mock import MagicMock

import pytest
from _pytest.logging import LogCaptureFixture
from confluent_kafka import Message, KafkaError
# Assuming your callback code above is saved in a file named `kafka_monitor.py`
from kafka_monitor import delivery_report

# @pytest.fixture
# def mock_message():
#     """Provides a basic mock Kafka message."""
#     msg = MagicMock(spec=Message)
#     msg.topic.return_value = "network-metrics"
#     msg.partition.return_value = 0
#     return msg

# @pytest.fixture
# def mock_kafka_error():
#     """Provides a basic mock Kafka error."""
#     err = MagicMock(spec=KafkaError)
#     err.code.return_value = KafkaError._UNKNOWN_REASON
#     err.__str__.return_value = "Generic Kafka error occurred"
#     return err

# def test_delivery_success(mock_message, caplog):
#     """Verifies that a successful delivery logs nothing (or success if uncommented)."""
#     with caplog.at_level("INFO"):
#         delivery_report(None, mock_message)
    
#     # Assert no errors were logged
#     assert "❌" not in caplog.text
#     assert "🚨" not in caplog.text

# def test_delivery_generic_error(mock_message, mock_kafka_error, caplog):
#     """Verifies handling of a generic Kafka error."""
#     with caplog.at_level("ERROR"):
#         delivery_report(mock_kafka_error, mock_message)
        
#     assert "❌ Message delivery failed with error code" in caplog.text

# def test_delivery_network_transport_error(mock_message, mock_kafka_error, caplog):
#     """Verifies handling of a network transport failure."""
#     mock_kafka_error.code.return_value = KafkaError._TRANSPORT
#     mock_kafka_error.__str__.return_value = "Connection reset by peer"
    
#     with caplog.at_level("ERROR"):
#         delivery_report(mock_kafka_error, mock_message)
        
#     assert "❌ Network transport failure" in caplog.text
#     assert "Connection reset by peer" in caplog.text

# def test_delivery_timeout_error(mock_message, mock_kafka_error, caplog):
#     """Verifies handling of a network timeout error."""
#     mock_kafka_error.code.return_value = KafkaError._TIMED_OUT
#     mock_kafka_error.__str__.return_value = "Queue full or broker timeout"
    
#     with caplog.at_level("CRITICAL"):
#         delivery_report(mock_kafka_error, mock_message)
        
#     assert "🚨 Network timeout" in caplog.text

# def test_delivery_all_brokers_down(mock_message, mock_kafka_error, caplog):
#     """Verifies handling when all brokers are down."""
#     mock_kafka_error.code.return_value = KafkaError._ALL_BROKERS_DOWN
    
#     with caplog.at_level("CRITICAL"):
#         delivery_report(mock_kafka_error, mock_message)
        
#     assert "🔥 Critical: All Kafka brokers are down" in caplog.text



@pytest.fixture
def mock_message() -> MagicMock:
    """Provides a basic mock Kafka message."""
    msg: MagicMock = MagicMock(spec=Message)
    msg.topic.return_value = "network-metrics"
    msg.partition.return_value = 0
    return msg

@pytest.fixture
def mock_kafka_error() -> MagicMock:
    """Provides a basic mock Kafka error."""
    err: MagicMock  = MagicMock(spec=KafkaError)
    err.code.return_value = KafkaError._UNKNOWN_REASON
    err.__str__.return_value = "Generic Kafka error occurred"
    return err

def test_delivery_success(mock_message: MagicMock, caplog: LogCaptureFixture) -> None:
    """Verifies that a successful delivery logs nothing (or success if uncommented)."""
    with caplog.at_level("INFO"):
        delivery_report(None, mock_message)
    
    # Assert no errors were logged
    assert "❌" not in caplog.text
    assert "🚨" not in caplog.text

def test_delivery_generic_error(mock_message: MagicMock, mock_kafka_error: MagicMock, caplog: LogCaptureFixture) -> None:
    """Verifies handling of a generic Kafka error."""
    with caplog.at_level("ERROR"):
        delivery_report(mock_kafka_error, mock_message)
        
    assert "❌ Message delivery failed with error code" in caplog.text

def test_delivery_network_transport_error(mock_message: MagicMock, mock_kafka_error: MagicMock, caplog: LogCaptureFixture) -> None:
    """Verifies handling of a network transport failure."""
    mock_kafka_error.code.return_value = KafkaError._TRANSPORT
    mock_kafka_error.__str__.return_value = "Connection reset by peer"
    
    with caplog.at_level("ERROR"):
        delivery_report(mock_kafka_error, mock_message)
        
    assert "❌ Network transport failure" in caplog.text
    assert "Connection reset by peer" in caplog.text

def test_delivery_timeout_error(mock_message: MagicMock, mock_kafka_error: MagicMock, caplog: LogCaptureFixture) -> None:
    """Verifies handling of a network timeout error."""
    mock_kafka_error.code.return_value = KafkaError._TIMED_OUT
    mock_kafka_error.__str__.return_value = "Queue full or broker timeout"
    
    with caplog.at_level("CRITICAL"):
        delivery_report(mock_kafka_error, mock_message)
        
    assert "🚨 Network timeout" in caplog.text

def test_delivery_all_brokers_down(mock_message: MagicMock, mock_kafka_error: MagicMock, caplog: LogCaptureFixture) -> None:
    """Verifies handling when all brokers are down."""
    mock_kafka_error.code.return_value = KafkaError._ALL_BROKERS_DOWN
    
    with caplog.at_level("CRITICAL"):
        delivery_report(mock_kafka_error, mock_message)
        
    assert "🔥 Critical: All Kafka brokers are down" in caplog.text
