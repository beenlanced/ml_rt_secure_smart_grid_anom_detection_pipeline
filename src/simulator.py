import asyncio
import json
import logging
import random
import time
from typing import Any, Dict, List, Optional

from confluent_kafka import KafkaError, Message, Producer


# Configuration constants
BOOTSTRAP_SERVERS = 'localhost:19092' # Initial address of local Kafka broker configuration to discover full cluster
TOPIC_NAME = 'smartgrid-telemetry' # Sets target Kafka log stream where telemetry data is gets appended
NUM_DEVICES = 1000  # Parallel virtual smart meters manage concurrently
EMIT_INTERVAL_SECONDS = 0.1  # 100ms standard grid frequency window - reporting rate of 10 telemetry updates per second

# Set up logging for production readiness
#logging.basicConfig(level=logging.INFO)
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
#logger = logging.getLogger(__name__)
logger = logging.getLogger("KafkaCallback")
# Configure the logger (Place this at the entry point of your application)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("SmartMeterSimulator")

# Kafka Delivery Callback for Network Performance Monitoring
def delivery_report(err: Optional[KafkaError], msg: Message) -> None:
    """
    Handles the result of a Kafka message production attempt with
    network error handling. Defines an asynchronous callback hook
    triggered by the Kafka background thread whenever an acknowlegement
    from the cluster succeeds or permanently fails.

    Args:
        err (Optional[KafkaError]): The error object if the delivery failed, or None if successful.
        msg (Message): The Kafka message object containing metadata like topic and partition.
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

async def kafka_flush_worker(producer: Producer) -> None:
    """
        Dedicated background loop to handle callbacks and clear network IO queues.
        Defines a long-running companion task responsible for servicing underlying
        native network buffers.

        Args:
            producer (Producer): Producer takes data (like the smart meter readings) and sends
                                 (publishes) it to a central sytem. 
    """
    while True:
        producer.poll(0.01)  # Check queues for completed events trigger delivery_reports. Process pkts for 10ms
        await asyncio.sleep(0.01)  # Yield control to allow virtual meter to generate additional messages

async def simulate_smart_meter(device_id: int, producer: Producer):
    """
        Simulates a single physical smart meter node tracking electrical telemetry.
    
        Args:
            device_id (int): The id of the of the smart meter
            producer (Producer): Producer takes data (like the smart meter readings) and sends
                                 (publishes)it to a central sytem 
    """

    # Establish a baseline grid state for this node
    meter_name = f"meter_{device_id:05d}"
    base_voltage = 120.0  # US standard residential voltage
    base_current = 15.0   # Amps

    # Log simulation startup
    logger.info(f"[{meter_name}] Initializing smart meter simulation node.")
    
    try:
        while True:
            # Inject minor natural time-series fluctuations (Gaussian noise)
            voltage = random.gauss(base_voltage, 0.5)
            current = random.gauss(base_current, 1.0)
            
            # Randomly inject a malicious cyber-anomaly (0.5% chance) - Anom Detect portion
            is_anomaly = random.random() < 0.005
            if is_anomaly:
                voltage = random.uniform(140.0, 160.0)  # Mimic severe real-grid malfunction of over-voltage spike
                current = random.uniform(0.0, 2.0) # Mimic low current real-grid malfunction
                # Log detected anomalies as warnings
                logger.warning(f"[{meter_name}] Cyber-anomaly injected! V={voltage:.2f}V, A={current:.2f}A")
 

            # Build structural data payload matching OT/IoT pipeline formats
            # normalize asset tags (device_id's) so that IDs from different gateways have a predictable format
            payload = {
                "timestamp": time.time(),
                "device_id": f"meter_{device_id:05d}", # (e.g., meter_00007)
                "metrics": {
                    "voltage_v": round(voltage, 2),
                    "current_a": round(current, 2),
                    "power_kw": round((voltage * current) / 1000.0, 3) #Calculates true power
                },
                "security_flag": int(is_anomaly) # Designate reading as an error or not. 
            }

            # Serialize data to string/bytes
            # Needed to convert Python Dict to standard JSON-formatted text string
            # encode coverts JSON into sequence of raw bytes (binary data)
            # Binary data needed because Network sockets/ IoT protocols transport bytes
            message_bytes = json.dumps(payload).encode('utf-8')

            # Retry loop for local buffer handling 
            while True:
                try:
                    # Produce message non-blockingly to Kafka/Redpanda
                    # sends IoT data payload to Apache Kafka cluster
                    producer.produce(
                        topic=TOPIC_NAME, 
                        key=payload["device_id"], #use device_id as the partition key to order specific meter pkts
                        value=message_bytes, 
                        callback=delivery_report
                    )
                    # Log successful metric generation (Debug level to avoid flooding stdout)
                    logger.debug(f"[{meter_name}] Telemetry payload produced successfully.")
                    break # Succes, break retry loop
                except BufferError:
                    # Catche instances where the local system buffers are full (max limit hit)
                    # Queue is full wait briefly 50 ms for flush_worker to clear it.
                    # Log local queue congestion
                    logger.warning(f"[{meter_name}] Local producer buffer full. Retrying in 50ms...")
                    await asyncio.sleep(0.05)
                except Exception as e:
                   # Alert catastrophic non-buffer exceptions(e.g., missing partition, serialization)
                   # Log catastrophic failures with traceback info
                   logger.critical(f"[{meter_name}] Permanent produce error: {e}", exc_info=True)
                   print(f"Permanent produce error on device {device_id}: {e}")
                   break

            await asyncio.sleep(EMIT_INTERVAL_SECONDS) #put current meter to sleep
        
    except asyncio.CancelledError:
        # Expected exit signal on shutdown
        # Graceful shutdown preventing unclean stack traces
        # Log clean shutdown event
        logger.info(f"[{meter_name}] Simulation task cancelled. Shutting down cleanly.")
        pass

async def main() -> None:
    """Initializes the Kafka producer pipeline, spawns background flusher workers, 
       and manages concurrent virtual smart grid device tasks.
    """
    # print(f"Initializing Kafka Producer connecting to {BOOTSTRAP_SERVERS}...")
    # Log Initializing Kafka Producer
    logger.info(f"Initializing Kafka Producer connecting to {BOOTSTRAP_SERVERS}...")

    # Define configuration dictionary with string keys and mixed value types
    producer_config: Dict[str, Any] = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        "queue.buffering.max.messages": 500000, #max packets to prevent memory starvation issues 
        "linger.ms": 10, # Artificially forces 10 ms pauses before batches to enable micro-bundling
        "compression.type": "snappy", #Compress streaming records via snappy to save CPU cycles
    }
    
    # Instantiate the confluent-kafka Producer client
    try:
        producer: Producer = Producer(producer_config)
        logger.info("Kafka Producer instantiated successfully.")
    except Exception as e:
        logger.error(f"Failed to instantiate Kafka Producer: {e}", exc_info=True)
        return

    #print(f"Launching {NUM_DEVICES} concurrent virtual smart grid devices...")
    logger.info(f"Launching {NUM_DEVICES} concurrent virtual smart grid devices...")

    # Start the async flusher worker in the background to prevent network blockages
    # Uses list comprehension to instantiate 1K independent smart meter simulatiors
    flush_task: asyncio.Task[None] = asyncio.create_task(
        kafka_flush_worker(producer)
    )

    # List comprehension to spawn and track all concurrent virtual device tasks
    device_tasks: List[asyncio.Task[None]] = [
        asyncio.create_task(simulate_smart_meter(i, producer))
        for i in range(NUM_DEVICES)
    ]

    try:
        # Keep the event loop running while all devices execute indefinitely
        # *argument unpacks the list of devices 
        await asyncio.gather(*device_tasks)
    except (KeyboardInterrupt, asyncio.CancelledError):
        # print("\n Shutting down simulator pipeline safely...")
        #Log the shutdown trigger event
        logger.warning("Pipeline interruption detected. Initiating safe shutdown sequence...")    
    finally:
        # Stop all devices from generating more data
        # Log the step-by-step cleanup process
        logger.info(f"Stopping {len(device_tasks)} device simulation tasks...")
        for task in device_tasks:
            task.cancel()
            
        # Await device cancellation completion while swallowing raised CancelledErrors
        await asyncio.gather(*device_tasks, return_exceptions=True)

        # Stop the background producer flusher worker task
        logger.info("Stopping background Kafka flush worker...")
        flush_task.cancel()

        # Final blocking drain of internal Kafka network buffers
        # Ensure any remaining msgs in buffers are fully delivered to Kafka clusters before application exits
        logger.info("Flushing remaining network packets in local Kafka buffer...")
        remaining_events = producer.flush(timeout=5) #cuts off after 5 seconds if a connection fails.

        #print("✨ Shutdown complete.")
        if remaining_events > 0:
            logger.warning(f"Flush timeout reached. {remaining_events} messages may have been dropped.")
        else:
            logger.info("Kafka buffer cleared completely.")
            
        logger.info("Grid simulator pipeline shutdown complete.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
