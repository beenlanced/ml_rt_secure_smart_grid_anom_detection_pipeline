import asyncio
import json
import logging
import random
import time
from typing import Optional

from confluent_kafka import KafkaError, Message, Producer

# Configuration constants
BOOTSTRAP_SERVERS = 'localhost:19092' #set aside unique port for Kafka streaming broker
TOPIC_NAME = 'smartgrid-telemetry'
NUM_DEVICES = 1000  # Scale up number of simulated smart meters as needed for performance testing
EMIT_INTERVAL_SECONDS = 0.1  # 100ms standard grid frequency window - reporting rate of 10 telemetry updates per second


# Set up logging for production readiness
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KafkaCallback")

# Kafka Delivery Callback for Network Performance Monitoring
def deliver_report(err: Optional[KafkaError], msg: Message) -> None:
    """
    Handles the result of a Kafka message production attempt with
    network error handling.

    Args:
        err: The error object if the delivery failed, or None if successful.
        msg: The Kafka message object containing metadata like topic and partition.
    """
    # Check if an error occurred during delivery
    if err is not None:
        error_code = err.code()
        
        # Handle specific network-related errors
        if error_code == KafkaError._TRANSPORT:
            logger.error(f"❌ Network transport failure (e.g., broker disconnected): {err}")
        elif error_code == KafkaError._TIMED_OUT:
            logger.critical(f"🚨 Network timeout. Message failed to reach broker in time: {err}")
        elif error_code == KafkaError._ALL_BROKERS_DOWN:
            logger.critical(f"🔥 Critical: All Kafka brokers are down or unreachable: {err}")
        else:
            # Fallback for all other Kafka errors
            logger.error(f"❌ Message delivery failed with error code {error_code}: {err}")
        return
        
    # Commented out to prevent terminal flooding during high-throughput tests
    # else:
    #     print(f"✅ Delivered to {msg.topic()} [{msg.partition()}]")

