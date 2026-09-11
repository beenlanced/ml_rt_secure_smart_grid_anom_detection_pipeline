from datetime import datetime, timezone
import logging
import json
import os
from pathlib import Path
import time
from typing import Any, Final, Optional

from config.logging_configs.mylogger import setup_production_logging
from dotenv import load_dotenv
import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException, Message
from psycopg2.extensions import connection as PgConnection
from psycopg2.extras import execute_values

# ==============================================================================
# Logging Configuration
# ==============================================================================
logger: logging.Logger = logging.getLogger("smartgrid.consumer")

# ==============================================================================
# Extract .env data
# ==============================================================================
# Get the directory where consumer.py lives (src/)
script_dir = Path(__file__).resolve().parent

# Go up one level to the root, then down into the config folder
env_path = script_dir.parent / "config" / ".env"

# Load the environment variables from the specific .env path
load_dotenv(dotenv_path=env_path)

# Extract the values
db = os.getenv("POSTGRES_DB")
db_user = os.getenv("POSTGRES_USER")
db_password = os.getenv("POSTGRES_PASSWORD")

# ==============================================================================
# Infrastructure & Database Configurations
# ==============================================================================
BOOTSTRAP_SERVERS: Final[str] = 'localhost:19092'
GROUP_ID: Final[str] = 'smartgrid-analytics-group'
TOPIC_NAME: Final[str] = 'smartgrid-telemetry'
DB_DSN: Final[str] = f"host=localhost dbname={db} user={db_user} password={db_password} port=5432"

BATCH_SIZE: Final[int] = 500  # Micro-batching writes to optimize database performance

# Type Alias for database row structures
type TelemetryRow = tuple[str, int, float, float, float, bool]

def create_db_connection() -> Optional[PgConnection]:
    """
    Establishes and returns a connection to the TimescaleDB instance.

    Returns:
        Optional[PgConnection]: A valid psycopg2 connection object, or None if connection fails.
    """
    try:
        conn: PgConnection = psycopg2.connect(DB_DSN)
        return conn
    except Exception as e:
        logger.error(f"Database connection failed: {e}", exc_info=True)
        return None

def insert_batch_with_retry(conn: PgConnection, batch: list[TelemetryRow], max_retries: int = 3) -> bool:
    """
    Executes a fast, bulk insert into the Hypertable with a retry mechanism.

    Args:
            conn (PgConnection): Active database connection wrapper.
            batch (list[TelemetryRow]): List of structured telemetry tuples to insert.
            max_retries(int): Maximum number of retries
    
    Returns:
        bool: True if the batch was written successfully, False otherwise.
    """
    query: Final[str] = """
        INSERT INTO grid_telemetry (timestamp, device_id, voltage_v, current_a, power_kw, security_flag)
        VALUES %s;
    """
    retries = 0
    backoff = 1.0  # start with a 1-second delay

    while retries < max_retries:
        try:
            with conn.cursor() as cursor:
                execute_values(cursor, query, batch)
            conn.commit()
            logger.debug("Successfully flushed batch of %d records to TimescaleDB.", len(batch))
            return True
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            retries += 1
            logger.warning(f"Database connection issue on write attempt {retries}/{max_retries}: {e}. Retrying in {backoff}s...")
            conn.rollback()
            time.sleep(backoff)
            backoff *= 2  # Exponential backoff
        except Exception as e:
            logger.error(f"Fatal error inserting batch into database: {e}", exc_info=True)
            conn.rollback()
            return False
            
    logger.error("Failed to write batch to database after maximum retries.")
    return False

def main() -> None:
    """
    Application entry point initializing the streaming consumer loop and pipeline.
    """
    db_conn: Optional[PgConnection] = None
    consumer: Optional[Consumer] = None
    data_batch: list[TelemetryRow] = []

    # 1. Initialize Database Connection
    try:
        db_conn = create_db_connection()
    except Exception as e:
        logger.critical("Fatal: Could not initialize database stream connection. Exiting...", exc_info=True)
        return

    # Check if the connection returned is valid
    if db_conn is None:
        logger.critical("Fatal: Database connection returned None. Exiting...")
        return


    # 2. Configure Kafka Consumer
    # Group IDs control offset tracking for access-management parity
    consumer_config: Final[dict[str, Any]] = {
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True,
        'fetch.min.bytes': 1024 * 64,  # Wait for 64KB of data to lower CPU context switching
        'linger.ms': 50
    }
    
    try:
        consumer: Consumer = Consumer(consumer_config)
        consumer.subscribe([TOPIC_NAME])
        logger.info("Consumer started. Listening for live stream telemetry on '%s'...", TOPIC_NAME)
    except Exception as e:
        logger.critical(f"Failed to instantiate or subscribe Kafka consumer: {e}", exc_info=True)
        if db_conn:
            db_conn.close()
        return

    #3. Stream Processing Loop
    try:
        while True:
            # Poll for new streaming network messages
            msg: Optional[Message] = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            
            # Parse message payload bytes to JSON
            try:
                payload: dict[str, Any] = json.loads(msg.value().decode('utf-8'))
                
                # Real-Time threshold check parsing metrics on-the-fly
                voltage: float = payload["metrics"]["voltage_v"]
                if voltage > 135.0:
                    logger.warning("Severe voltage anomaly detected on node: %s (%.2fV)", payload['device_id'], voltage)

                # Transform payload map into a database tuple row matching timezone expectations
                dt_object: str = datetime.fromtimestamp(payload["timestamp"], tz=timezone.utc).isoformat()
                
                row: TelemetryRow = (
                    dt_object,
                    payload["device_id"],
                    voltage,
                    payload["metrics"]["current_a"],
                    payload["metrics"]["power_kw"],
                    payload["security_flag"]
                )
                
                data_batch.append(row)
                
                # If micro-batch is full, flush to TimescaleDB
                if len(data_batch) >= BATCH_SIZE:
                    success = insert_batch_with_retry(db_conn, data_batch)
                    if not success:
                        logger.critical("Database pipeline broke down permanently. Terminating loop to protect data tracking offsets.")
                        break
                    data_batch.clear()
                    
            except Exception as e:
                logger.error(f"Error parsing incoming message stream: {e}", exc_info=True)

    except KeyboardInterrupt:
        logger.info("Consumer stopping manually via KeyboardInterrupt...")
    except Exception as e:
        logger.error(f"Fatal runtime exception encountered in consumer loop: {e}", exc_info=True)
    finally:
        # 4. Fail-safe isolated shutdown steps
        # Flush remaining messages in buffer
        if data_batch and db_conn:
            logger.info("Flushing final remaining %d records before disconnection...", len(data_batch))
            try:
                insert_batch_with_retry(db_conn, data_batch, max_retries=1)
            except Exception:
                 logger.error("Could not flush final batch on shutdown; database unreachable.")
            #insert_batch(db_conn, data_batch)
        
        if consumer:
            try:
                consumer.close()
                logger.info("Kafka consumer cleanly disconnected.")
            except Exception:
                logger.error("Failed to close Kafka consumer cleanly.", exc_info=True)

        if db_conn:
            try:
                db_conn.close()
                logger.info("Database pool closed cleanly.")
            except Exception:
                logger.error("Failed to close database connection cleanly.", exc_info=True)

if __name__ == "__main__":
    # Instantiate the non-blocking queue logging architecture,
    # the bootstrap code first
    setup_production_logging()

    try:
        main()
    except KeyboardInterrupt:
        pass
