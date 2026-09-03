import asyncio
import json
import logging
import random
import time

from aiokafka import AIOKafkaProducer

from config.logging_configs.mylogger import setup_production_logging

# ==============================================================================
# Logging Configuration
# ==============================================================================
# Note: Do not execute basicConfig here anymore!
logger: logging.Logger = logging.getLogger("smartgrid.simulation")

# ==============================================================================
# Infrastructure & Simulation Configurations
# ==============================================================================
KAFKA_BOOTSTRAP_SERVER: str = "localhost:19092"
KAFKA_TOPIC: str = "smartgrid.telemetry"
TOTAL_METERS: int = 10000
INTERVAL_SEC: float = 0.1  # 100 milliseconds boundary

# Type Alias
type JsonPayload = dict[str, any]

async def generate_meter_reading(meter_id: int) -> JsonPayload:
    """
    Generates ultra-lightweight telemetry to save network bandwidth.

    Args:
        meter_id (int): The id of the of the smart meter

    Returns:
        (JsonPayload/dict) : random realisitic smart grid smart meter payload values.

    """
    return {
        "m_id": meter_id,
        "v": round(random.uniform(115.0, 125.0), 2),  # Voltage reading
        "c": round(random.uniform(5.0, 15.0), 2),    # Current reading
        "t": int(time.time() * 1000)                 # Epoch millisecond timestamp
    }

async def smart_meter_worker(worker_id: int, queue: asyncio.Queue[tuple[str, JsonPayload]]) -> None:
    """
    Worker responsible for a subset of meters to balance event loop overhead.

    Args:
        worker_id (int): Total meters split into one o 100 concurrently running workers 
                        (e.g., worker#0 simulates meters 0-99, worker#2 101-199, etc.))

        queue (asyncio.Queue[tuple[str, JsonPayload]]): queue to handle messages

    """
    meters_per_worker: int = TOTAL_METERS // 100  
    start_idx: int = worker_id * meters_per_worker
    end_idx: int = start_idx + meters_per_worker

    logger.debug("Worker %d initialized for meter range %d-%d", worker_id, start_idx, end_idx)

    while True:
        start_time: float = time.monotonic()
        
        for meter_id in range(start_idx, end_idx):
            reading: JsonPayload = await generate_meter_reading(meter_id)
            await queue.put((str(meter_id), reading))
        
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
    efficiently onver the wire to Kafka/Redpanda

    Args:
        dispatcher_id (int): network-dispatch (publisher) pipelines 
        queue (asyncio.Queue[tuple[str, JsonPayload]]): queue to handle messages.

    """
    producer: AIOKafkaProducer = AIOKafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVER,
        client_id=f"smartgrid-producer-{dispatcher_id}",
        batch_size=262144, 
        linger_ms=10, 
        compression_type="zstd", 
        acks=1, 
        max_request_size=5242880,
        buffer_memory=67108864
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

            #Transfors native dict text string into compressed raw newtork bytes
            serialized_payload: bytes = json.dumps(payload).encode('utf-8')

            #Send message to the TOPIC, push onto the async nonblocking socket stream
            await producer.send(
                topic=KAFKA_TOPIC, 
                value=serialized_payload, 
                key=key.encode('utf-8')
            )
            queue.task_done()
            processed_count += 1
            
            # Periodically log throughput stats to avoid flooding stdout (Every 5 seconds)
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
        logger.error("Dispatcher #%d encountered pipeline error: %s", dispatcher_id, e, exc_info=True)
    finally:
        await producer.stop()
        logger.info("Dispatcher #%d connection pool securely closed.", dispatcher_id)

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
        for i in range(100)
    ]
    
    # Spin up multiple pipeline dispatchers to clear the queue out to Redpanda/Kafka
    logger.info("Spawning 4 tuned Kafka publisher pipelines...")
    dispatchers: list[asyncio.Task[None]] = [
        asyncio.create_task(kafka_delivery_pipeline(i, telemetry_queue)) 
        for i in range(4)
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
