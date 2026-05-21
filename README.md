# Real-Time End-to-End Data Engineering Pipeline
## 🏗️ Architecture Overview

This project demonstrates a fully containerized, real-time data streaming pipeline designed to ingest, process, and store high-velocity user data continuously.

The data flows seamlessly through the following stages:


Data Generation: A custom Python client simulates live events by polling the randomuser.me API for user profile records.


Orchestration: Apache Airflow coordinates the execution flow, systematically triggering the ingestion script to act as a Kafka producer. Airflow utilizes a PostgreSQL database to manage its metadata state.


Ingestion & In-Flight Queuing: Apache Kafka handles message ingestion on a dedicated users_created topic, with Apache Zookeeper managing cluster state and broker stability.


Stream Processing: Apache Spark (PySpark) acts as the real-time consumer. Leveraging Spark Structured Streaming, it listens to the Kafka topic, consumes the raw streams, and applies a strict schema structure dynamically.


Analytical Storage (Sink): The structured records are continuously written to Apache Cassandra, a distributed NoSQL database heavily optimized for high-write throughput and rapid log storage.


Infrastructure Deployment: The entire ecosystem is fully containerized and orchestrated using a single, monolithic Docker Compose environment.

📊 Streaming & System Metrics

Data Source: Synthetic, real-time user event logs from the randomuser.me API.


Ingestion Velocity: Configured to push continuous batch updates every minute to simulate constant transactional traffic.


Storage Layer Strategy: Leverages a distributed NoSQL ring structure to handle high-write stream processing without traditional relational database locks