# Building Stream Processing Frameworks

Topic: Real-time analytics with Apache Flink or Faust.
Action: Write a stream processing job to ingest the Kafka topic. Calculate a 10-second rolling average of grid voltage. Trigger an immediate alert if the voltage spikes beyond standard deviations.

To transition from data ingestion to persistent storage and live stream analysis will require both a `database schema` optimized for time-series structures and a `consumer script` to process the real-time telemetry.

## Schema.sql (TimescaleDB schema script)

This file describes the database schema which is a logical container or blueprint that defines how data is organized, structured, and secured within a database

As this project uses [PostgreSQL]("https://www.postgresql.org/about/"), the schema acts like a folder inside the database. It allows you to group related objects (like tables, views, and stored procedures) together for organizational and security purposes.

This SQL script sets up a highly optimized, time-series data storage system using `TimescaleDB` (an extension for PostgreSQL specifically optimized to handle time-series and event data). It gives you the "best of both worlds": the massive scalability and performance of a dedicated time-series database, paired with the full relational capabilities, reliability, and familiar SQL syntax of standard PostgreSQL. [Github-timescaledb]("https://github.com/timescale/timescaledb"). It is designed specifically for handling massive volumes of streaming IoT data, such as electrical grid readings, while minimizing disk space and maintaining fast query speeds.

It uses hypertables, an advanced PostgreSQL database table, designed to automatically partition time-series and event data into smaller, manageable pieces called chunks. Hypertables are often used for IoT and sensors to store high-frequency device metrics like temperature, humidity or GPS coordinates.

## Consumer.py
