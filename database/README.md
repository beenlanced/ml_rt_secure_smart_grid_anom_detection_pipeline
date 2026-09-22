# Building Stream Processing Frameworks

Topic: Real-time analytics with Apache Flink or Faust.
Action: Write a stream processing job to ingest the Kafka topic. Calculate a 10-second rolling average of grid voltage. Trigger an immediate alert if the voltage spikes beyond standard deviations.

To transition from data ingestion to persistent storage and live stream analysis will require both a `database schema` optimized for time-series structures and a `consumer script` to process the real-time telemetry.

### How The Pieces work together

[Producer] -> Emits millions of raw metrics (m_id, v, c, t)
|
v
[Redpanda/Kafka] -> Streams message queues non-stop
|
v
[Consumer.py] -> Batches and writes row data
|
v
======================= TIMESCALEDB BOUNDARY =======================
|
+---> [schema.sql]
| Creates the raw "grid_telemetry" hypertable.
| Compresses data older than 2 hours to save 90% disk space.
|
+---> [Continuous Aggregation Script]
Sits on top of the raw table.
Automatically rolls up raw points into 5-minute dashboard metrics.

---

## Schema.sql (TimescaleDB schema script)

This file describes the database schema which is a logical container or blueprint that defines how data is organized, structured, and secured within a database

As this project uses [PostgreSQL]("https://www.postgresql.org/about/"), the schema acts like a folder inside the database. It allows you to group related objects (like tables, views, and stored procedures) together for organizational and security purposes.

This Ingestion Foundation file creates the raw, high-throughput hypertable (grid_telemetry) where your Python consumer directly writes incoming stream data. It focuses on organizing raw data, fast index lookups, and disk compression.

This SQL script sets up a highly optimized, time-series data storage system using `TimescaleDB` (an extension for PostgreSQL specifically optimized to handle time-series and event data). It gives you the "best of both worlds": the massive scalability and performance of a dedicated time-series database, paired with the full relational capabilities, reliability, and familiar SQL syntax of standard PostgreSQL. [Github-timescaledb]("https://github.com/timescale/timescaledb"). It is designed specifically for handling massive volumes of streaming IoT data, such as electrical grid readings, while minimizing disk space and maintaining fast query speeds.

It uses hypertables, an advanced PostgreSQL database table, designed to automatically partition time-series and event data into smaller, manageable pieces called chunks. Hypertables are often used for IoT and sensors to store high-frequency device metrics like temperature, humidity or GPS coordinates.

The Schema.sql script sets up a query indexing to speed up data retrieval using a `composite index`.

Next, the script peforms compressions settings such that it prepares the hypertable to use TimescaleDB's columnar compression engine. `compress_segmentby = 'device_id'` tells the database to group rows together by their specific device ID before compressing them. Because data from the same device changes predictably over time, grouping it this way allows compression algorithms to achieve up to a 90% reduction in disk space. It triggers a fundamental architectural shift in how PostgreSQL stores data: it transforms historical, time-series data from a traditional row-oriented store into a highly optimized column-oriented store. (Data grouping with arrary-based storage)

Finally, and automation policy is inacted to automate the lifecycle of the data. It schedules a background job that checks for data chunks older than 2 hours and automatically compresses them. This creates a "hot/warm" data architecture: the most recent 2 hours of data remain uncompressed for ultra-fast modifications and real-time streaming inserts, while anything older is compressed to save massive amounts of storage.

---

## The Continuous Aggregation Script (The Analytics Layer):

The second part of the database setup.

This is a separate view that sits on top of the raw table. It tells the database engine to run in the background, take the millions of raw data points from your hypertable, and pre-calculate them into aggregated intervals (like 5-minute averages).

---

## Real-time Consumer & Ingestion script: consumer.py

This script acts as your streaming consumer. It continuously listens and reads streaming smart grid telemetry data from a message queue (Redpanda/Kafka), decodes the incoming byte data, extracts metrics, computes moving calculations, and writes it in high-performance batches that are inserted as the records into the time-series database, TimescaleDB, for ideal database performance.

### The Core Responsibilities of `consumer.py`

`Consuming Stream Data`:

It connects to a Kafka/Redpanda broker (i.e., the individual server that runs the Kafka or Redpanda software-- the physical node responsible for receiving, storing, and serving stream data) running locally, subscribes to a specific topic (smartgrid-telemetry), and listens for new messages. A topic is a named category or channel used to organize and store a specific feed of messages. It is an append-only log, where every new message gets tacked onto the very end of the line.

When your `consumer.py` script starts up:

1. It looks for the broker at `localhost:19092` to establish a network connection.

2. It asks that broker to subscribe to the specific topic named `smartgrid-telemetry`.

3. The broker checks its internal storage logs for that topic and starts streaming those specific telemetry messages back to your script.

`Real-time Alerting`:

It inspects data on the fly and immediately logs a warning if a severe voltage anomaly occurs (defined as voltage exceeding 135.0V).

`Data Transformation`:

It decodes raw message bytes into JSON, extracts specific nested metrics, and converts epoch timestamps into UTC ISO strings.

`Micro-Batch Ingestion`:
Instead of writing to the database message-by-message (which is slow), it stores rows in a memory buffer until it reaches 500 records. Once full, it writes them all to TimescaleDB at once to optimize database performance.

### How it is doing it (The Technical Workflow)

The script relies on several key external libraries: confluent_kafka for data streaming, and psycopg2 for PostgreSQL/TimescaleDB operations.

[Kafka Topic] ---> poll() ---> Memory Buffer ---> Batch Insert (500) ---> [TimescaleDB]
|
(Check Voltage > 135V) ---> Log Warning Alert

#### Step A: Infrastructure Setup (main)

1. Database Connection: The script calls `create_db_connection()`, using a Data Source Name (DSN) string to open a connection to PostgreSQL/TimescaleDB.

2. Kafka Initialization: It configures a `Consumer` object. Performance tuning settings like `fetch.min.bytes` (64KB) and `linger.ms` (50) are configured here to limit CPU overhead and reduce context switching.

3. Subscription: It subscribes to the `smartgrid-telemetry` topic and enters an infinite `while True` loop.

#### Step B: The Stream Processing Loop

1. Polling: n an Apache Kafka queue (technically a distributed commit log), polling is the mechanism where a consumer actively requests and fetches a batch of data from a Kafka topic. In the `consumer.py`, every second (`timeout=1.0`), the script polls Kafka for a new message. If no message is found, it safely loops back to poll again.

2. Parsing & Validation:
   1. It decodes the incoming message bytes using UTF-8 and parses it via `json.loads`.

   2. It reads payload["metrics"]["voltage_v"]. If it exceeds `135.0`, it fires off a `logger.warning` alert.

3. Data Mapping: It restructures the JSON payload into a strict Python tuple matching the `TelemetryRow` type alias:`(timestamp, device_id, voltage_v, current_a, power_kw, security_flag)`.

4. Buffering & Flushing: The tuple is appended to a list named `data_batch`. When `len(data_batch) >= 500`, it triggers `insert_batch()`.

#### Step C: High-Performance Ingestion (insert_batch)

- To avoid the latency of 500 individual `INSERT` queries, the script uses `execute_values()` from `psycopg2.extras`.

- This function constructs a single SQL bulk insert block (`INSERT INTO grid_telemetry VALUES %s;`).

- If successful, it runs `conn.commit()`. If a database error occurs, it rolls back the micro-transaction (`conn.rollback()`) and logs the fault without crashing the entire service.

#### Step D: Graceful Teardown

- If the script receives a shutdown command (`KeyboardInterrupt` or a terminal stop signal), it breaks out of the loop safely.

- Before shutting down, the `finally`: block checks if there are any trailing messages left in `data_batch` (e.g., 230 records that hadn't hit the 500 limit yet) and flushes them to the database.

- It then safely closes the Kafka consumer and the database connection pool to avoid leaking system connections.
