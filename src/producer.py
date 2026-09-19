import asyncio
import json
import logging
import math
import random
import sys
import time

from aiokafka import AIOKafkaProducer

from config.logging_configs.mylogger import setup_production_logging

# ==============================================================================
# Logging Configuration
# ==============================================================================
logger: logging.Logger = logging.getLogger("smartgrid.simulation")

# ==============================================================================
# Infrastructure & Simulation Configurations
# ==============================================================================
KAFKA_BOOTSTRAP_SERVER: str = "localhost:19092"
KAFKA_TOPIC: str = "smartgrid-telemetry"
TOTAL_METERS: int = 10000
INTERVAL_SEC: float = 0.1  # 100 milliseconds boundary
NUM_WORKERS: int = 100
NUM_DISPATCHERS: int = 4

# Shared state to prevent duplicate expensive datetime operations
CURRENT_LOCAL_HOUR: float = 12.0

# Type Alias
type JsonPayload = dict[str, any]

def generate_meter_reading(meter_id: int) -> JsonPayload:
    """
    Generates ultra-lightweight telemetry to save network bandwidth.

    Args:
        meter_id (int): The id of the of the smart meter

    Returns:
        (JsonPayload/dict) : random realisitic smart grid smart meter payload values.

    """
    current_time = time.time()
    # Mocking out standard residential current draws and power factor variables
    voltage = round(random.uniform(115.0, 125.0), 2)
    current = round(random.uniform(5.0, 15.0), 2)
    power_kw = round((voltage * current * 0.92) / 1000.0, 3)

    # Establish a baseline grid state for this node
    meter_name = f"meter_{meter_id:05d}"
    base_voltage = 120.0  # US standard residential voltage

    # Assign different baseline behaviors based on a simulated appliance mix
    # Some homes are heavy users, some are low users
    user_scale = random.uniform(0.7, 1.5)

    current_time = time.time()
    # 1. Diurnal Load Curve (Pulling from memory instead of OS timezone queries)
    # Generates a smooth, periodic cosine wave normalized to a range of [0.0, 1.0]
    # based on the shared variable CURRENT_LOCAL_HOUR:
    #  - (math.pi / 12) maps the 24-hour daily timeline into a standard 2*pi radian wave cycle.
    #  - The '0.5 - 0.5 * cos(...)' structure flips and normalizes the raw cosine values.
    #  - This ensures the curve hits its peak load (1.0) at Noon (12:00) when grid demand is high,
    #    and drops to its lowest draw (0.0) at Midnight (00:00/24:00) during off-peak sleep hours.
    time_effect = 0.5 - 0.5 * math.cos(math.pi * CURRENT_LOCAL_HOUR / 12)

    # Base current draws heavily depend on the time of day
    expected_current = (5.0 + (25.0 * time_effect)) * user_scale

    # 2. Continuous Time-Series Noise (Voltage drops when current spikes)
    # In real grids, high current draw causes a slight voltage sag
    voltage_sag = (expected_current / 30.0) * 1.5
    voltage = random.gauss(base_voltage - voltage_sag, 0.3)
    current = max(0.1, random.gauss(expected_current, 0.5)) # Prevent negative current

    # 3. Realistic Power Factor (AC Physics)
    # Residential power factors usually fluctuate between 0.85 and 0.97
    power_factor = random.uniform(0.88, 0.95)

    # 4. Cyber-Anomaly Injection (0.5% chance)
    is_anomaly = random.random() < 0.005
    if is_anomaly:
        # Severe grid malfunction or attack: Voltage surge + sudden trip (zero current)
        voltage = random.uniform(145.0, 155.0)
        current = random.uniform(0.0, 0.5)

        # Total phase collapse - voltage and current waves are almost completely out of sync
        # misaligning by an extreme phase angle of roughly 78.5 degrees
        power_factor = 0.2
        logger.warning(f"[{meter_name}] Cyber-anomaly injected! V={voltage:.2f}V, A={current:.2f}A")

    # True Active Power (kW) = (V * I * PF) / 1000
    power_kw = (voltage * current * power_factor) / 1000.0

    # Build structural data payload matching OT/IoT pipeline formats
    # normalize asset tags (device_id's) so that IDs from different gateways have a predictable format
    payload = {
        "timestamp": current_time,
        "device_id": meter_name,
        "metrics": {
            "voltage_v": round(voltage, 2),
            "current_a": round(current, 2),
            "power_kw": round(power_kw, 3),
            "power_factor": round(power_factor, 2)
        },
        "security_flag": int(is_anomaly)
    }

    return payload

async def smart_meter_worker(worker_id: int, queue: asyncio.Queue[tuple[str, JsonPayload]]) -> None:
    """
    Worker responsible for a subset of meters to balance event loop overhead.
    Simulates a block of smart meters tightly aligned to the 100ms interval window

    Args:
        worker_id (int): Total meters split into one of 100 concurrently running workers 
                        (e.g., worker#0 simulates meters 0-99, worker#2 101-199, etc.))

        queue (asyncio.Queue[tuple[str, JsonPayload]]): queue to handle messages

    """
    meters_per_worker: int = TOTAL_METERS // NUM_WORKERS  
    start_idx: int = worker_id * meters_per_worker
    end_idx: int = start_idx + meters_per_worker

    logger.debug("Worker %d initialized for meter range %d-%d", worker_id, start_idx, end_idx)

    # Cache queue local references to optimize loop lookups
    # avoids Python having to look up the attribute on the object 10,000 times
    put_nowait = queue.put_nowait

    while True:
        start_time: float = time.monotonic()
        
        # Safe data dropping if full queue.
        # If Kafka slows down, better to skip a few readings.
        # Make sure the simulation continues running real-time sync with clock
        for meter_id in range(start_idx, end_idx):
            reading: JsonPayload = generate_meter_reading(meter_id)
            try:
                put_nowait((str(meter_id), reading))
            except asyncio.QueueFull:
                # Log periodically or pass silently to maintain real-time execution speeds
                logger.warning("Queue full! Dropping reading for meter %d", meter_id)

        elapsed: float = time.monotonic() - start_time
        sleep_time: float = max(0.0, INTERVAL_SEC - elapsed) #monitor execution speed
        
        # Log a warning if a single worker thread breaches its allotted 100ms window
        if elapsed > INTERVAL_SEC:
            logger.warning(
                "Worker %d processing loop delayed: took %.2fms (Limit: %.2fms)",
                worker_id, elapsed * 1000, INTERVAL_SEC * 1000
            )

        await asyncio.sleep(sleep_time)

async def kafka_delivery_pipeline(dispatcher_id: int, queue: asyncio.Queue[tuple[str, JsonPayload]]) -> None:
    """
    High-throughput tuned Kafka producer pipeline with metrics logging. It drains 
    the memory queue rapidly, prepares structured objects, and dispatches them
    efficiently over the wire to Kafka/Redpanda.

    Args:
        dispatcher_id (int): network-dispatch (publisher) pipelines 
        queue (asyncio.Queue[tuple[str, JsonPayload]]): queue to handle messages.

    """
    # Note Offloading string/JSON encoding to Kafka's background execution lifecycle
    producer: AIOKafkaProducer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        client_id=f"smartgrid-producer-{dispatcher_id}",
        max_batch_size=262144, 
        linger_ms=10, 
        compression_type="zstd", 
        acks=1, 
        max_request_size=5242880,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=lambda k: k.encode('utf-8')
    )

    logger.info("Starting Kafka Delivery Pipeline Dispatcher #%d...", dispatcher_id)
    await producer.start() #connect to Kafka server
    logger.info("Tuned Producer Pipeline #%d successfully running.", dispatcher_id)

    processed_count: int = 0
    last_reported_time: float = time.monotonic()

    try:
        while True:
            #Pull raw tuples from the queue
            key, payload = await queue.get()

            # OPTIMIZATION: Use send() to avoid blocking the event loop per message.
            # This passes bytes immediately to aiokafka's internal buffer for optimal batching.
            await producer.send(topic=KAFKA_TOPIC, value=payload, key=key)

            queue.task_done()
            processed_count += 1

            # Periodically log throughput stats to avoid flooding stdout (Every 5 seconds)
            # Non-blocking performance diagnostic logging
            current_time: float = time.monotonic()
            if current_time - last_reported_time >= 5.0:
                throughput: float = processed_count / (current_time - last_reported_time)
                logger.info(
                    "Dispatcher #%d Throughput Status: Sent %d records (~%.2f msg/sec)", 
                    dispatcher_id, processed_count, throughput
                )
                processed_count = 0
                last_reported_time = current_time

    except asyncio.CancelledError:
        logger.info("Dispatcher #%d received cancellation signal. Cleaning up resources...", dispatcher_id)
    except Exception as e:
        logger.error("Dispatcher #%d encountered error:", dispatcher_id, e, exc_info=True)
    finally:
        await producer.stop()
        logger.info("Dispatcher #%d connection pool closed.", dispatcher_id)

async def main() -> None:
    """
    Application entry point initializing workers, queues, and dispatch pipelines.
    """
    logger.info("Initializing Smart Grid Simulation cluster configuration...")

    # Build bounded storage buffer to avoid Out-Of-Memory (OOM)
    telemetry_queue: asyncio.Queue[tuple[str, JsonPayload]] = asyncio.Queue(maxsize=500000)

    # Spin up worker clusters to handle data emission
    logger.info("Spawning 100 concurrent smart meter simulation workers...")
    workers: list[asyncio.Task[None]] = [
        asyncio.create_task(smart_meter_worker(i, telemetry_queue)) #event place in event loop
        for i in range(NUM_WORKERS)
    ]

    # Spin up multiple pipeline dispatchers to clear the queue out to Redpanda/Kafka
    logger.info("Spawning 4 tuned Kafka publisher pipelines...")
    dispatchers: list[asyncio.Task[None]] = [
        asyncio.create_task(kafka_delivery_pipeline(i, telemetry_queue)) 
        for i in range(NUM_DISPATCHERS)
    ]

    logger.info("Simulation matrix fully active. Pushing metrics data cluster...")
    try:
        await asyncio.gather(*workers, *dispatchers) #register all 104 running loops directly into main system
    except Exception as e:
        logger.critical("Fatal crash caught in simulation loop matrix: %s", e, exc_info=True)

if __name__ == "__main__":
    # Instantiate the non-blocking queue logging architecture,
    # the bootsrap code first
    setup_production_logging()

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
