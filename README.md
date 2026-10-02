# Real-Time Fraud Transaction Detection

## Overview

This project builds a real-time fraud detection pipeline for credit-card transactions. It combines:

- a Kafka-based event streaming layer,
- historical transaction replay for simulation,
- a stream-processing foundation for feature generation,
- and a future model-serving workflow for real-time fraud scoring.

The goal is to transform raw transaction history into a streaming architecture that can support near-real-time risk detection and downstream decisioning.

## Current Development Status

This repository is in an active development phase. The project has moved from planning into a working local streaming foundation.

### Development Update — 2026-10-03

- Integrated Redis with the Spark streaming flow and completed an initial end-to-end integration test.
- Streamed transaction cases with `producer.py` using a 1-second delay, processed the real-time data, and stored the basic metrics `batch_id`, `total_records`, and `fraud_count` in Redis.
- Checked the values with `HGETALL fraud:metrics:latest` and confirmed that the metrics updated as expected.
- This was a flow and integration check. `fraud_count` is included only for this experiment; in the production-ready flow it should be written to Redis after model prediction, rather than before prediction.
- Next, compute the required metrics and store them in Redis, and update the fraud-count handling to follow the post-prediction flow.

### Development Update ? 2026-09-30

- Tuned the Kafka producer rate and Spark's `maxOffsetsPerTrigger` to find a working balance: the producer sent 2,000 records per second while Spark processed batches capped at 10,000 offsets.
- Spark kept up with the stream and wrote the processed data to Parquet.
- Processed nearly 300,000 data points, which will be used to train the machine-learning model in the next step.

### Development Update ? 2026-09-28

- Configured a local Kafka broker using Docker in [fraud_streaming/kafka/docker-compose.yml](fraud_streaming/kafka/docker-compose.yml)
- Added a transaction replay producer that reads the CSV dataset and publishes rows to the `transaction-stream` topic in [real_time_data_producer/producer.py](real_time_data_producer/producer.py)
- Defined the dataset path and runtime project configuration in [config.py](config.py)
- Documented Kafka topic operations and cleanup steps in [commands.md](commands.md)
- Established the project structure for upcoming stream-processing and feature-generation work

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
├── README.md
├── commands.md
├── config.py
├── execution_steps.md
├── requirements.txt
├── data/
│   ├── processed/
│   └── raw_input/
│       └── credit_card_transactions.csv
├── dev_env/
├── fraud_streaming/
│   └── kafka/
│       └── docker-compose.yml
├── jobs/
│   └── fraud_streaming_job.py
├── real_time_data_producer/
│   └── producer.py
├── winutils/
└── .gitignore
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

## Tech Stack

- Python
- Kafka
- Docker
- PySpark / streaming processing foundation
- Pandas / CSV ingestion
- future: feature store, model training, and online inference tools

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
