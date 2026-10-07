# Real-Time Fraud Transaction Detection

## Tools & Skills

[![Python](https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white)](https://www.python.org/)
[![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Pandas](https://img.shields.io/badge/Pandas-150458?logo=pandas&logoColor=white)](https://pandas.pydata.org/)
[![Redis](https://img.shields.io/badge/Redis-DC382D?logo=redis&logoColor=white)](https://redis.io/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white)](https://www.docker.com/)
[![Jupyter](https://img.shields.io/badge/Jupyter-F37626?logo=jupyter&logoColor=white)](https://jupyter.org/)

The project uses Python, Kafka, Docker, PySpark, Pandas, Redis, and Jupyter notebooks for transaction ingestion, stream processing, analytics, and feature selection. Grafana is part of the planned metrics dashboard workflow. Model training and online inference are upcoming stages.

## Contents

- [Overview](#overview)
- [Current Development Status](#current-development-status)
- [High-Level Architecture](#high-level-architecture)
- [Project Workflow](#project-workflow)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Local Setup](#local-setup)
- [Kafka Management Commands](#kafka-management-commands)
- [Tools & Skills](#tools--skills)
- [Notes](#notes)
- [Next Milestones](#next-milestones)

---

## Overview

This project builds a real-time fraud detection pipeline for credit-card transactions. It combines:

- a Kafka-based event streaming layer,
- historical transaction replay for simulation,
- a stream-processing foundation for feature generation,
- and a future model-serving workflow for real-time fraud scoring.

The goal is to transform raw transaction history into a streaming architecture that can support near-real-time risk detection and downstream decisioning.

## Current Development Status

This repository is in an active development phase. The project has moved from planning into a working local streaming foundation.

<details open>
<summary><strong>Development Update - 2026-10-07</strong></summary>

- Completed feature selection in [model_development/feature_selection.ipynb](model_development/feature_selection.ipynb) using fill-rate analysis, near-zero-variance screening, and information value (IV) filtering.
- Evaluated 326,799 transactions across 28 columns. Fill-rate analysis found no columns below the 90% threshold; near-zero-variance screening flagged one column, and IV filtering retained 17 features at the 0.2 threshold.
- The combined report retains 17 predictor features and the `is_fraud` target, while dropping 10 predictors. The target is preserved as the label for supervised learning.
- Next step: use the selected predictor features for model development and training.

</details>

<details>
<summary><strong>Development Update - 2026-10-04</strong></summary>

- Added a separate Spark streaming session in `fraud_streaming/spark/feature_analysis.ipynb` to periodically read the current feature-store window from Redis and compute analytical metrics.
- Added a dedicated Redis analytics client to store the latest computed metrics for Grafana.
- Completed the Grafana and Redis configuration. The next step is to build the Grafana dashboard using the published metrics.

</details>

<details>
<summary><strong>Development Update - 2026-10-03</strong></summary>

- Integrated Redis with the Spark streaming flow and completed an initial end-to-end integration test.
- Streamed transaction cases with `producer.py` using a 1-second delay, processed the real-time data, and stored the basic metrics `batch_id`, `total_records`, and `fraud_count` in Redis.
- Checked the values with `HGETALL fraud:metrics:latest` and confirmed that the metrics updated as expected.
- This was a flow and integration check. `fraud_count` is included only for this experiment; in the production-ready flow it should be written to Redis after model prediction, rather than before prediction.
- Next, compute the required metrics and store them in Redis, and update the fraud-count handling to follow the post-prediction flow.

</details>

<details>
<summary><strong>Development Update - 2026-09-30</strong></summary>

- Tuned the Kafka producer rate and Spark's `maxOffsetsPerTrigger` to find a working balance: the producer sent 2,000 records per second while Spark processed batches capped at 10,000 offsets.
- Spark kept up with the stream and wrote the processed data to Parquet.
- Processed nearly 300,000 data points, which will be used to train the machine-learning model in the next step.

</details>

<details>
<summary><strong>Development Update - 2026-09-28</strong></summary>

- Configured a local Kafka broker using Docker in [fraud_streaming/kafka/docker-compose.yml](fraud_streaming/kafka/docker-compose.yml)
- Added a transaction replay producer that reads the CSV dataset and publishes rows to the `transaction-stream` topic in [real_time_data_producer/producer.py](real_time_data_producer/producer.py)
- Defined the dataset path and runtime project configuration in [config.py](config.py)
- Documented Kafka topic operations and cleanup steps in [commands.md](commands.md)
- Established the project structure for upcoming stream-processing and feature-generation work

</details>

### In Progress / Planned

- Kafka consumer implementation and streaming transformation logic
- rolling feature calculation for transaction behavior
- online/offline feature store integration
- model training and fraud scoring service
- monitoring, drift detection, and alerting

> The repository currently has a functioning ingestion layer, while the rest of the fraud-detection pipeline is being developed in subsequent milestones.

---

## High-Level Architecture

```text
CSV / Parquet transaction data
            |
            v
Python event producer
            |
            v
Kafka topic: transaction-stream
            |
            v
Stream processing / feature generation
            |
            v
Feature store + storage layer
            |
            v
Offline training / model registration
            |
            v
Real-time inference service
            |
            v
Risk decision: approve / reject / challenge
```

## Project Workflow

### 1. Data ingestion

Historical transaction data is loaded from the CSV file and re-sent as serialized Kafka events.

### 2. Stream processing

The incoming stream is intended to compute rolling or time-window features such as transaction frequency, amount behavior, and user activity patterns.

### 3. Feature store and model training

The design includes storing generated features for historical training and online lookups during inference.

### 4. Real-time prediction

The final system should score each transaction in near real time and decide whether it should be approved, rejected, or challenged.

---

## Repository Structure

```text
real_time_fraud_transaction_detection/
|-- README.md
|-- commands.md
|-- config.py
|-- execution_steps.md
|-- requirements.txt
|-- data/
|   |-- processed/
|   `-- raw_input/
|       `-- credit_card_transactions.csv
|-- dev_env/
|-- fraud_streaming/
|   `-- kafka/
|       `-- docker-compose.yml
|-- jobs/
|   `-- fraud_streaming_job.py
|-- real_time_data_producer/
|   `-- producer.py
|-- winutils/
`-- .gitignore
```

---

## Prerequisites

- Python 3.10+
- Docker Desktop
- Kafka-compatible local setup
- Python virtual environment (optional but recommended)

---

## Local Setup

### 1. Start Kafka

From the project root:

```bash
docker compose -f fraud_streaming/kafka/docker-compose.yml up -d
```

### 2. Activate the Python environment

```bash
./dev_env/Scripts/activate
```

On PowerShell, use:

```powershell
.\dev_env\Scripts\Activate.ps1
```

### 3. Run the producer

```bash
python real_time_data_producer/producer.py --interval 1.0
```

This reads the CSV from the configured raw dataset path and publishes each row to the `transaction-stream` topic.

### 4. Consume messages from Kafka

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --topic transaction-stream --bootstrap-server localhost:9092 --from-beginning
```

---

## Kafka Management Commands

The project includes operational notes for topic cleanup and retention resets in [commands.md](commands.md).

Common examples:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic transaction-stream

docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --topic transaction-stream --partitions 1 --replication-factor 1
```

---

## Notes

This project is intended to evolve from a data replay setup into a complete streaming fraud detection system. The current repository establishes the base architecture and local Kafka ingestion pipeline, which is the foundation for the next stages of stream processing, model training, and inference.

## Next Milestones

1. Implement Kafka consumer / stream processing job
2. Generate rolling transaction features
3. Add feature store integration
4. Train fraud model on historical data
5. Expose real-time inference endpoint
6. Add monitoring and drift detection
