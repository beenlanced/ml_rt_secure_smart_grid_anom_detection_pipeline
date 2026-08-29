# SRC files notes

### simulator.py

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

### To Use the Docker file

To start the infrastructure created by the docker compose file.

- Open your terminal or command prompt.
- Navigate to the directory containing your docker compose file file using the cd command:
  cd /path/to/your/directory

```bash
docker compose up -d
```

Check that it works by issuing

```bash
docker compose ps
```

Look at the STATUS column. You will see it transition from (health: starting) to (healthy) once Redpanda and TimescaleDB are fully booted up and ready for action

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

## TESTS

```bash
pytest -v tests_simulator_new.py

```

### tests_simulator.py

- `LogCaptureFixture`: Imported from `_pytest.logging` to explicitly reference pytest's internal native logging diagnostic framework capture class (caplog).

- `MagicMock`: Used for test metrics instead of Mock since `confluent-kafka` properties rely on nested internal structures and magic methods (like `.kwargs` or `.__str__()`).

-`asyncio.Task[None]`: Denotes async loop handles running tasks that terminate gracefully without returning an explicit scalar value.

-`None`: Explicitly assigned to test methods and setup scripts to satisfy strict checking behaviors required by standard configurations like `mypy` or `pyright`.
