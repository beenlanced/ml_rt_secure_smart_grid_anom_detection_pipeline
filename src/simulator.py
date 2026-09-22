import asyncio
import json
import logging
import math
import random
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

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

# Type Alias matching producer.py collection syntax styles
JsonPayload = Dict[str, Any]


async def time_keeper_worker() -> None:
    """
    Dedicated background worker that updates the global local hour once 
    every second instead of doing it 10,000 times a second across individual meters.
    Prevents expensive system-level C call that queries the operating system for 
    each of the meters.
    """
    global CURRENT_LOCAL_HOUR
    try:
        while True:
            t_struct = time.localtime()
            CURRENT_LOCAL_HOUR = t_struct.tm_hour + (t_struct.tm_min / 60.0) + (t_struct.tm_sec / 3600.0)
            await asyncio.sleep(1.0)
    except asyncio.CancelledError:
        logger.info("Time keeper system background task stopped cleanly.")


def generate_meter_reading(meter_id: int) -> JsonPayload:
    """
    Generates ultra-lightweight telemetry to save network bandwidth.

    Args:
        meter_id (int): The id of the of the smart meter

    Returns:
        (JsonPayload/dict) : random realisitic smart grid smart meter payload values.
    """
    current_time = time.time()
    
    # Establish a baseline grid state for this node
    meter_name = f"meter_{meter_id:05d}"
    base_voltage = 120.0  # US standard residential voltage

    # Assign different baseline behaviors based on a simulated appliance mix
    user_scale = random.uniform(0.7, 1.5)

    # 1. Diurnal Load Curve (Pulling from memory instead of OS timezone queries)
    time_effect = 0.5 - 0.5 * math.cos(math.pi * CURRENT_LOCAL_HOUR / 12)

    # Base current draws heavily depend on the time of day
    expected_current = (5.0 + (25.0 * time_effect)) * user_scale

    # 2. Continuous Time-Series Noise (Voltage drops when current spikes)
    voltage_sag = (expected_current / 30.0) * 1.5
    voltage = random.gauss(base_voltage - voltage_sag, 0.3)
    current = max(0.1, random.gauss(expected_current, 0.5))

    # 3. Realistic Power Factor (AC Physics)
    power_factor = random.uniform(0.88, 0.95)

    # 4. Cyber-Anomaly Injection (0.5% chance)
    is_anomaly = random.random() < 0.005
    if is_anomaly:
        voltage = random.uniform(145.0, 155.0)
        current = random.uniform(0.0, 0.5)
        power_factor = 0.2
        logger.warning(f"[{meter_name}] Cyber-anomaly injected! V={voltage:.2f}V, A={current:.2f}A")

    # True Active Power (kW) = (V * I * PF) / 1000
    power_kw = (voltage * current * power_factor) / 1000.0

    # Build structural data payload matching OT/IoT pipeline formats
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


async def smart_meter_worker(worker_id: int, queue: asyncio.Queue[Tuple[str, JsonPayload]]) -> None:
    """
    Worker responsible for a subset of meters to balance event loop overhead.
    """
    meters_per_worker: int = TOTAL_METERS // NUM_WORKERS
    start_idx: int = worker_id * meters_per_worker
    end_idx: int = start_idx + meters_per_worker

    logger.debug(f"Worker {worker_id} initialized for meter range {start_idx}-{end_idx}")
    put_nowait = queue.put_nowait

    try:
        while True:
            start_time: float = time.monotonic()
            
            for meter_id in range(start_idx, end_idx):
                reading: JsonPayload = generate_meter_reading(meter_id)
                try:
                    put_nowait((str(meter_id), reading))
                except asyncio.QueueFull:
                    logger.warning(f"Queue full! Dropping reading for meter {meter_id}")
            
            # OPTIMIZATION: Yield control to event loop to allow Kafka pipelines
            # to transmit buffered metrics without thread starvation.
            await asyncio.sleep(0)

            elapsed: float = time.monotonic() - start_time
            sleep_time: float = max(0.0, INTERVAL_SEC - elapsed)
            
            if elapsed > INTERVAL_SEC:
                logger.warning(
                    f"Worker {worker_id} processing loop delayed: took {elapsed * 1000:.2f}ms (Limit: {INTERVAL_SEC * 1000:.2f}ms)"
                )

            await asyncio.sleep(sleep_time)
            
    except asyncio.CancelledError:
        logger.info(f"Worker {worker_id} simulation task cancelled. Shutting down cleanly.")


async def kafka_delivery_pipeline(dispatcher_id: int, queue: asyncio.Queue[Tuple[str, JsonPayload]]) -> None:
    """
    High-throughput tuned Kafka producer pipeline with metrics logging.
    """
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

    logger.info(f"Starting Kafka Delivery Pipeline Dispatcher #{dispatcher_id}...")
    try:
        await producer.start()
        logger.info(f"Tuned Producer Pipeline #{dispatcher_id} successfully running.")
    except Exception as e:
        logger.error(f"Failed to start Producer Pipeline #{dispatcher_id}: {e}", exc_info=True)
        return

    processed_count: int = 0
    last_reported_time: float = time.monotonic()

    try:
        while True:
            key, payload = await queue.get()
            try:
                await producer.send(topic=KAFKA_TOPIC, value=payload, key=key)
            finally:
                queue.task_done()
                
            processed_count += 1

            current_time: float = time.monotonic()
            if current_time - last_reported_time >= 5.0:
                throughput: float = processed_count / (current_time - last_reported_time)
                logger.info(
                    f"Dispatcher #{dispatcher_id} Throughput Status: Sent {processed_count} records (~{throughput:.2f} msg/sec)"
                )
                processed_count = 0
                last_reported_time = current_time

    except asyncio.CancelledError:
        logger.info(f"Dispatcher #{dispatcher_id} received cancellation signal. Cleaning up resources...")
    except Exception as e:
        logger.error(f"🚨 Dispatcher #{dispatcher_id} encountered a pipeline error: {e}", exc_info=True)
    finally:
        await producer.stop()
        logger.info(f"Dispatcher #{dispatcher_id} connection pool closed.")


async def main() -> None:
    """
    Application entry point initializing workers, queues, and dispatch pipelines.
    """
    logger.info("Initializing Smart Grid Simulation cluster configuration...")

    telemetry_queue: asyncio.Queue[Tuple[str, JsonPayload]] = asyncio.Queue(maxsize=500000)

    # Infrastructure workers initialization
    time_task: asyncio.Task[None] = asyncio.create_task(time_keeper_worker())

    logger.info(f"Spawning {NUM_WORKERS} concurrent smart meter simulation workers...")
    workers: List[asyncio.Task[None]] = [
        asyncio.create_task(smart_meter_worker(i, telemetry_queue))
        for i in range(NUM_WORKERS)
    ]

    logger.info(f"Spawning {NUM_DISPATCHERS} tuned Kafka publisher pipelines...")
    dispatchers: List[asyncio.Task[None]] = [
        asyncio.create_task(kafka_delivery_pipeline(i, telemetry_queue)) 
        for i in range(NUM_DISPATCHERS)
    ]

    logger.info("Simulation matrix fully active. Pushing metrics data cluster...")
    try:
        await asyncio.gather(time_task, *workers, *dispatchers) 
    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.warning("Pipeline interruption detected. Initiating safe shutdown sequence...")
    except Exception as e:
        logger.critical(f"🔥 Fatal crash caught in simulation loop matrix: {e}", exc_info=True)
    finally:
        logger.info("Stopping all background simulation and infrastructure tasks...")
        
        # Cancel infrastructure timers
        time_task.cancel()
        
        # Stop worker threads and dispatch loops
        for worker_task in workers:
            worker_task.cancel()
        for dispatcher_task in dispatchers:
            dispatcher_task.cancel()

        # Clean teardown aggregation
        await asyncio.gather(time_task, *workers, *dispatchers, return_exceptions=True)
        logger.info("Grid simulator pipeline shutdown complete.")


if __name__ == "__main__":
    setup_production_logging()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
