# SRC files notes

### simulator.py

(asyncio Simulation): Implements an asynchronous producer loop feeding a high-performance memory queue. It simulates real-world electrical patterns by generating normal baselines with random, intermittent anomaly spikes.

- [asyncio library](https://realpython.com/async-io-python/) - provides ability to run concurrent code, you can run multiple tasks at the same time without making your computer wait around doing nothing. It helps your program multitask efficiently, especially when it is waiting on the internet or a database.

- [confluent_kafka](https://www.youtube.com/watch?v=06iRM1Ghr1k)
  - Used for a distributed log (topic) system providing events in real-time. Enrichment in DB parlance means joins.
  - Producers are write-only connections

- [telemetry](https://www.merriam-webster.com/simple/telemetry)
  The process of using special equipment to take measurements of something (such as pressure, speed, or temperature) and send them by radio to another place.

- [Smart Grid Frequency Window](https://whatissmartenergy.org/smart-grid-and-power-quality)
  A common smart grid frequency window is typically maintained within ±0.050 Hz from 60 Hz in North America or 50 Hz in Europe. This narrow tolerance is crucial for ensuring the stability and reliability of the power system. Here frequency refers to the sumber of cycles per second.

  A smart grid's 100 ms emit interval represents a reporting rate of 10 telemetry updates per second. In a 60 Hz electrical system, this 100 ms window captures precisely 6 complete AC power cycles per report (since one 60 Hz cycle takes 16.67 ms), allowing smart meters to aggregate and stream localized frequency stability data rapidly.

  Understanding the relationship.
  - Cycle Count: At 60 cycles per second, a 100 ms collection window spans (60 x 0.1 = 6) full cycles of the alternating current waveform.

  - Resolution vs. Speed: Emitting data every 100 ms enables monitoring devices to spot short-term **rate-of-change of frequency (RoCoF)** and **feed fast frequency response (FFR)** automation without overwhelming communication networks with raw, cycle-by-cycle (16.67 ms) analog samples.

  - Averaging Window: High-speed sensors use the 100 ms block to compute a stable rolling average of the instantaneous frequency, filtering out background noise while still detecting real grid disturbances.

  n a 60 Hz power system, serious electrical faults cause instant changes in the voltage and current waves. The 100 ms emit interval is the perfect balance for high-speed automated defense systems. It is fast enough to catch a disaster in real time, but slow enough to send stable data.

  Here is exactly how grid operators and automated systems use this 100 ms window to detect and fix faults.

1. Spotting Fast Frequency Drops Using **rate-of-change of frequency (RoCoF)**
   When a major power plant fails, grid frequency drops instantly.

- The 100 ms Advantage: Advanced grid sensors look at the Rate of Change of Frequency (RoCoF).

- The Math: Because 100 ms covers exactly 6 power cycles, the sensor can compare these 6 cycles to the previous 6 cycles.

- The Action: If the frequency drops drastically between two 100 ms updates, the system flags a massive fault before the grid crashes.

2. Identifying Phasor and Phase Angle Jumps

A fault like a downed power line changes the "phase angle" of the electricity.

- Phasor Measurement Units (PMUs): These high-speed grid sensors measure the exact alignment of the 60 Hz wave.

- The 100 ms Advantage: PMUs stream data 10 to 30 times per second. A 100 ms interval captures these sudden shifts in the wave alignment.

- The Action: A sudden jump in the phase angle between two 100 ms reports tells computers exactly where a line broke.

3. Triggering Fast Frequency Response (FFR)

Traditional power plants take seconds or minutes to respond to a fault. Modern grids use batteries.

- The 100 ms Advantage: Large scale battery storage systems can inject power into the grid in under 50 milliseconds.

- The Action: By receiving a fault alert in the 100 ms data packet, automated software can trigger utility-scale batteries to dump power into the grid instantly, stopping a blackout.

4. Isolating the Fault (Breaker Coordination)

Grids use circuit breakers to cut off broken lines so the rest of the city keeps its power.

The 100 ms Advantage: Mechanical breakers take about 50 to 80 ms to physically open.

The Action: The 100 ms data window matches this physical timeline. It allows the grid's computer brain to see the fault, confirm which breaker opened, and reroute power in the next 100 ms window.

- [Home Appliance Voltage North America](https://whatissmartenergy.org/smart-grid-and-power-quality)
  Power quality refers to electricity that consistently meets the agreed-upon specifications for optimal and efficient use in home electronics. In North America, home appliances and electronics are designed to operate within a range of 106 volts to 127 volts of alternating current (AC). However, equipment operates most efficiently in a range of 114-126 volts (quantity of electricity), which is the standard for delivered voltage in North America.

- [Operational Technology (OT) and Industrial IoT (IIO) pipelines, data payload formats]()

1. The Core Structural RequirementsTo make raw sensor data usable for IT and cloud systems, a standard OT/IoT pipeline payload must structure four essential data layers:

- Timestamp: The exact microsecond or millisecond the physical event occurred at the edge.

- Asset Context (Metadata): Where the data came from (e.g., Enterprise → Site → Area → Line → Machine).

- Telemetry Data (Metrics): The actual reading (Value), its unique tag identifier (ID), and its data type (Float, Int, Boolean).

- Data Quality State: A flag showing if the sensor reading is "Good," "Bad," or "Stale/Uncertain".

- Key Logging Implmentations

* `Contextual Tagging`: Every log string prefixes [{meter_name}]. This is critical when running thousands of meters concurrently so you can trace individual device lifecycles.

* `logger.debug()` for Telemetry: Pushing data out happens continuously. Using `DEBUG` ensures your main logs aren't choked by thousands of "success" messages during production.

* `logger.warning()` for Congestion/Anomalies: Highlights when the system generates fake attacks or experiences **BufferError** bottlenecks that need infrastructure attention.

* `exc_info=True` on Critical Errors: Turning this on instructs the logger to capture the entire system traceback stack, showing you exactly where a script-breaking crash occurred.

## Docker Compose file

(Kafka Infrastructure) : Contains a docker-compose.yml optimized using Redpanda (a C++ alternative to Kafka that scales without JVM tuning memory traps).

### Make sure to create .env file in the same directory as the docker compose

Use the `.env.example` template tailored for the project. This file serves as a blueprint showing which keys are required without exposing actual sensitive passwords or configuration data.

To get everything running smoothly using this template, you or anyone else cloning the project should follow these steps:

- Step 1: Copy the example file to create the active configuration file.

```bash
cp .env.example .env
```

- Step 2: Open the newly created `.env` file and replace `your_strong_password_here` with a real, secure password.

- step 3: Now you are able to start containers using Docker Compose statements.

### Update your .gitignore

To completely ensure that no one accidentally pushes their private passwords to your Git repository, make sure your .gitignore file contains the following lines:

```text
# Ignore local environment deployment settings
.env

# Keep the template tracked in Git
!.env.example
```

### About the Docker File

This Docker Compose file sets up a data streaming and storage backbone typically used for IoT, telemetry, or real-time monitoring infrastructure. It spins up a high-performance [Redpanda]("https://www.redpanda.com/") messaging broker and a [TimescaleDB]("https://en.wikipedia.org/wiki/TimescaleDB") time-series database, configuring them to run in isolated containers that can communicate with each other and your host machine.

- Redpanda

Redpanda is a modern, high-performance streaming data platform that is fully API-compatible with Apache Kafka. Developers use it as a drop-in replacement for Kafka because it is written in C++ (instead of Java) and operates on a thread-per-core architecture.

- `Healthcheck`: settings in Docker Compose configuration acts as the medical checkup for your streaming broker. It performs a strict evaluation timeline:
  - `interval: 5s`: Docker runs rpk cluster info every 5 seconds.
  - `timeout: 3s`: If the command takes longer than 3 seconds to respond, Docker treats that individual attempt as a failure.
  - `retries: 5`: When the container first starts up, it is in a "starting" state. Docker gives it 5 consecutive failed checks (spanning roughly 25 seconds) to fully wake up before officially declaring the container `unhealthy`.

  Unlike a generic process check (which just asks your operating system "Is the Redpanda program open?"), `rpk cluster info` asks the application "Are you actually functioning and ready to accept data?"

- TimescaleDB (Time-Series Database)

  TimescaleDB is an extension of PostgreSQL optimized for fast storage and analysis of time-stamped data.
  - `image`: timescale/timescaledb:latest-pg15: Uses the official TimescaleDB image built on PostgreSQL 15.

  - `environment`: Configures the default database credentials:
    - `Database Name`: smartgrid
    - `User`: postgres
    - `Password`: securepassword123

  - `ports`: Maps port **5432** from the container to your local computer, allowing standard PostgreSQL clients (like pgAdmin or DBeaver) to connect.

  - `volumes`: Defines a named volume called tsdata.

  It mounts to `/var/lib/postgresql/data` inside the database container. This ensures that even if you stop, delete, or update the database container, your stored data is safe and won't be lost.

- `Implicit Network`: Because no specific network is defined, Docker Compose automatically creates a default virtual bridge network. Redpanda can communicate with TimescaleDB internally using their container names (redpanda and smartgrid-db) as hostnames.

- `healthcheck`: Uses the `pg_isready` command to ensure the database is actively accepting connections before letting dependent services rely on it.

## Producer

The companion `aiokafka` producer is finely tuned with:

- `linger_ms=10` to micro-batch events on the wire. Delays delivery by up to 10ms to let data gather. Prevents sending 100,000 individual network packets per second.

- `batch_size=262144` caps ad data packet structure chunck size at 256KB. Packs thousands of smart meter metrics into a single optimized TCP transfer

- `compression_type="zstd"` to minimize high network traffic without choking CPU. Compresses text-heavy JSON telemetry streams. Shrinks total data footprint on yu disk and network pipe by up to 70%

- `acks=1` for high-throughput leader acknowledgment profiles. Acknolwedtes message receipt when leader Redpanda broker accepts it. Drastically decreases transmission round-trip wait time.

---

### Testing the Smart Grid streaming setup: docker-compose.yml, simulator.py, and producer.py

Will need to execute the components in a specific order:

1. stand up your infrastructure container, and then
2. run your high-throughput Python simulation scripts.

#### Starting the Infrastructure (Redpanda Broker) - Producer.py-Stream Execution Workflow

Follow this step-by-step pipeline execution order to stand up the simulation environment and verify the configurations

- 1. `Spin up the Container Infrastructure`

Ensure your local terminal path matches the location of your `docker-compose.yml file` (i.e., navigate to the directory containg the compose file), then create and run the streaming containers in detached mode:

```bash
#docker compose up -d
docker compose up -d --pull always
```

`--pull always`
This starts **Redpanda** (listening locally on port 19092) and **TimescaleDB** (on port 5432).

- 2. Confirm Infrastructure Health

Before pushing massive message volumes, ensure the stream broker's medical checkup status passes successfully:

```bash
docker compose ps
```

Verify that both `smartgrid-redpanda` and `smartgrid-db` report an (healthy) status state.

- 3. Execute the Streaming Script

Run the high-throughput `producer` application from your host environment by opening a different terminal
and executing:

```bash
python -m src.producer
```

- 4 Monitor Non-Blocking Logging Outputs
  Open a secondary terminal split to actively view your decoupled streaming outputs across both standard channels and queryable JSON structures:
  - Watch Standard Terminal Logs (INFO and below via stdout):

  ```bash
  tail -f logs/app_log.jsonl | grep -v "INFO"
  ```

  - Watch Cyber-Anomalies & Warnings (stderr):

  ```bash
  tail -f logs/app_log.jsonl | grep -v "WARNING"
  ```

  - Verify Structured JSON Production Logging Format:

  ```bash
  head -n 5 logs/app_log.jsonl
  ```

#### Starting the Infrastructure (Redpanda Broker) - Stream.py-Execution Workflow

- 1. Establish the Infrastructure Baseline

The simulator.py file points to `BOOTSTRAP_SERVERS = 'localhost:19092'`. Make sure your `Redpanda` cluster is fully up and running to receive data.

```bash
# 1. Spin up your smart-grid streaming environment
docker compose up -d

# 2. Verify containers are marked as healthy
docker compose ps
```

- 2. `Auto-Create the Target Kafka Topic`

`simulator.py` routes metrics to a topic named `smartgrid-telemetry`. To ensure smooth streaming without reliance on broker auto-creation rules, create the topic explicitly using `Redpanda's` built-in CLI tool (rpk):

```bash
docker exec -it smartgrid-redpanda rpk topic create smartgrid-telemetry --partitions 3
```

Using 3 partitions allows your 1,000 parallel virtual devices to stream concurrently without bottlenecks.

- 3. Run the Stream Execution Test

Execute the unified script from your local Python environment by opening a terminal and navigating to the directory where `simulator.py` exists and typing:

```bash
python -m src.simulator
```

Upon launching, your main terminal will immediately remain silent on stdout for default logs. This happens because the updated non-blocking filter (`NonErrorFilter`) and `stdout` stream handler route standard INFO events quietly to your background system threads.

- 4. Validate Logging Channels & Stream Output

Open a second terminal window to verify that your `QueueHandler` and background threads are routing traffic appropriately without stalling the execution loop:

- Monitor the Stuctured JSON Log Stream

Watch the logs populate in real time to verify that the custom `AppJSONFormatter` is appending data properly:

```bash
tail -f logs/app_log.jsonl
```

- Confirm the Module Namespace Name

Verify that the entries contain your updated module tag. A valid structured JSON log record will look like this:

```json
{
  "timestamp": "2026-09-04T00:47:42.609025+00:00",
  "level": "DEBUG",
  "logger": "smartgrid.simulation",
  "module": "simulator",
  "function": "simulate_smart_meter",
  "line": 133,
  "thread_name": "MainThread",
  "message": "[meter_00990] Telemetry payload produced successfully."
}
```

- Test Cyber-Anomaly Alerts (stderr)

Your script injects a malicious grid anomaly roughly 0.5% of the time. These alerts bypass the standard filter and hit stderr as WARNING logs via your detailed formatter layout. Isolate these specific errors to ensure your network filters work:

```bash
tail -f logs/app_log.jsonl | grep "WARNING"
```

- 5. Verify Data Delivery on the Live Broker

     To prove that the stream isn't just logging locally but is actively passing network traffic through `Redpanda`, read live packets directly out of the cluster's log stream:

```bash
docker exec -it smartgrid-redpanda rpk topic consume smartgrid-telemetry --num 5
```

if successful, you will see the raw, uncompressed operational telemetry payloads printed to your console window:

```json
{
  "topic": "smartgrid-telemetry",
  "key": "meter_00012",
  "value": "{\"timestamp\": 1788482818.7060359, \"device_id\": \"meter_00012\", \"metrics\": {\"voltage_v\": 119.44, \"current_a\": 14.0, \"power_kw\": 1.672}, \"security_flag\": 0}",
  "timestamp": 1788482818706,
  "partition": 0,
  "offset": 4
}
```
