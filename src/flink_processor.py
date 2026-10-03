import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
import sys
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
from pyflink.common import WatermarkStrategy, Types, RestartStrategies
from pyflink.datastream import StreamExecutionEnvironment, DataStream, CheckpointingMode
from pyflink.datastream.connectors.kafka import KafkaSource
from pyflink.datastream.connectors.jdbc import JdbcSink, JdbcConnectionOptions, JdbcExecutionOptions
from pyflink.common.serialization import SimpleStringSchema

# Imports configuration layout matching the simulator pipeline
from config.logging_configs.mylogger import setup_production_logging

# ==============================================================================
# Logging Configuration
# ==============================================================================
logger: logging.Logger = logging.getLogger("smartgrid.flink_processor")

# ==============================================================================
# Extract .env data
# ==============================================================================
# Get the directory where consumer.py lives (src/)
script_dir = Path(__file__).resolve().parent

# Go up one level to the root, then down into the config folder
env_path = script_dir.parent / "config" / ".env"

# Load the environment variables from the specific .env path
if env_path.exists():
    load_dotenv(dotenv_path=env_path)
else:
    logger.warning(f".env file not found at {env_path}. Relying on system environment variables.")

# Extract the values
DB_NAME: str = os.getenv("POSTGRES_DB", "smartgrid")
DB_USER: str = os.getenv("POSTGRES_USER", "postgres")
DB_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "password123")
DB_HOST: str = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT: str = os.getenv("POSTGRES_PORT", "5432")

# ==============================================================================
# Infrastructure & Database Configurations
# ==============================================================================
KAFKA_BOOTSTRAP_SERVER: str = os.getenv("KAFKA_BOOTSTRAP_SERVER", "localhost:19092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "smartgrid-telemetry")
KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "flink-grid-processors")

JDBC_URL: str = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_DRIVER: str = "org.postgresql.Driver"

# Explicit Type Alias matching database schema layout metrics (7 Fields total)
TelemetryTuple = Tuple[str, str, float, float, float, float, int]


def parse_json_string(json_str: str) -> Optional[TelemetryTuple]:
    """
    Parses raw Kafka string messages into a structured Flink-compatible Tuple.
    Extracts explicit database fields mapped from the simulator runtime metadata.

    Args:
        json_str (str): The raw JSON payload coming from the Kafka source engine.

    Returns:
        Optional[TelemetryTuple]: Clean telemetry data row for TimescaleDB,
                                  or None if the packet is corrupted.
    """
    try:
        data: Dict[str, Any] = json.loads(json_str)
        
        # Convert Unix float timestamp to an ISO string for TimescaleDB compatibility
        dt_str: str = datetime.fromtimestamp(data["timestamp"], tz=timezone.utc).isoformat()
        
        # Field mapping extracted directly from simulator payload structures
        device_id: str = str(data["device_id"])
        voltage_v: float = float(data["metrics"]["voltage_v"])
        current_a: float = float(data["metrics"]["current_a"])
        power_kw: float = float(data["metrics"]["power_kw"])
        power_factor:float = float(data["metrics"]["power_factor"])
        security_flag: int = int(data["security_flag"])
        
        return dt_str, device_id, voltage_v, current_a, power_kw, power_factor, security_flag

    except Exception as e:
        logger.error(f"Dropping corrupted stream packet: {e}", exc_info=True)
        return None


def run_flink_pipeline() -> None:
    """
    Application entry point initializing Flink streams, transforms, and JDBC engine sinks.
    """
    logger.info("Initializing Flink Distributed Processing Pipeline Engine...")

    # 1. Initialize the Flink execution environment
    env: StreamExecutionEnvironment = StreamExecutionEnvironment.get_execution_environment()

    # HARDENING: Enable Flink Checkpointing for Exact-Once state management & failure recovery
    # Flink will save state snapshot every 10,000ms (10 seconds)
    env.enable_checkpointing(10000, CheckpointingMode.EXACTLY_ONCE)
    env.get_checkpoint_config().set_checkpoint_timeout(60000)
    env.get_checkpoint_config().set_max_concurrent_checkpoints(1)
    
    # HARDENING: Define Failure Handling Restart Matrix strategy
    env.set_restart_strategy(
        RestartStrategies.fixed_delay_restart(
            restart_attempts=3, 
            delay_between_attempts=5000  # 5 seconds delay between retries
        )
    )

   # HARDENING: Fallback to environment variables or let cluster-level managers handle scaling
    env_parallelism = os.getenv("FLINK_PARALLELISM")
    if env_parallelism:
        env.set_parallelism(int(env_parallelism))
        logger.info(f"Execution parallelism locked from env environment: {env_parallelism}")
    else:
        logger.info("Relying on Flink cluster deployment standard parallelism settings.")

    # 2. Configure the Kafka/Redpanda Source Engine
    kafka_source: KafkaSource = KafkaSource.builder() \
        .set_bootstrap_servers(KAFKA_BOOTSTRAP_SERVER) \
        .set_topics(KAFKA_TOPIC) \
        .set_group_id(KAFKA_GROUP_ID) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # 3. Ingest the data stream
    raw_stream: DataStream = env.from_source(kafka_source, WatermarkStrategy.no_watermarks(), "RedpandaSource")

    # 4. Process and Transform Data on-the-fly
    processed_stream: DataStream = raw_stream \
        .map(parse_json_string, output_type=Types.TUPLE([
            Types.STRING(),  # timestamp
            Types.STRING(),  # device_id
            Types.FLOAT(),   # voltage_v
            Types.FLOAT(),   # current_a
            Types.FLOAT(),   # power_kw
            Types.FLOAT(),   # power_factor
            Types.INT()      # security_flag
        ])) \
        .filter(lambda row: row is not None)

    # Real-Time production tracking via structured log debug pipelines
    if logger.getEffectiveLevel() == logging.DEBUG:
        processed_stream.print(msg="Flink Stream Engine Routing -> ")

    # 5. Define the TimescaleDB Java Database Connectivity (JDBC) Sink Engine
    jdbc_sink = JdbcSink.sink(
        "INSERT INTO grid_telemetry (timestamp, device_id, voltage_v, current_a, power_kw, power_factor, security_flag) VALUES (?, ?, ?, ?, ?, ?, ?);",
        Types.TUPLE([Types.STRING(), Types.STRING(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.FLOAT(), Types.INT()]),
        JdbcConnectionOptions.JdbcConnectionOptionsBuilder() \
            .with_url(JDBC_URL) \
            .with_driver_name(JDBC_DRIVER) \
            .with_user_name(DB_USER) \
            .with_password(DB_PASSWORD) \
            .build(),
        JdbcExecutionOptions.builder() \
            .with_batch_size(500) \
            .with_batch_interval_ms(100) \
            .with_max_retries(3) \
            .build()
    )

    # 6. Pipe the live stream into the database sink
    processed_stream.add_sink(jdbc_sink)

    logger.info("Simulation streaming fabric fully engaged. Executing Flink matrix topology...")
    try:
        env.execute("SmartGrid-TSDB-Pipeline")
    except Exception as e:
        logger.critical(f"Fatal crash caught in Flink pipeline loop: {e}", exc_info=True)


if __name__ == "__main__":
    # Setup production environments mirroring the simulator configurations
    setup_production_logging()
    run_flink_pipeline()
