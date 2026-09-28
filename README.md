## End-to-End Real-Time Transaction Risk Scoring Pipeline

This project implements an end-to-end real-time transaction risk scoring pipeline.
The pipeline takes historical transaction data, replays it as a real-time event stream,
generates rolling features, stores them in an online feature store, trains a machine
learning model using historical features, and serves real-time risk predictions through
a FastAPI inference service.

### Pipeline Overview

The pipeline consists of the following main stages:

1. **Event Replay & Ingestion**
   - Historical CSV/Parquet transaction data is replayed using a Python event producer.
   - Relative deltas are calculated to simulate real-time events.
   - Events are published to a Kafka/Redpanda `transaction-stream` topic.

2. **Stream Processing**
   - The incoming transaction stream is processed using PySpark Streaming or Bytewax.
   - Rolling features are calculated over 10-minute and 1-hour windows.

3. **Feature Storage & Persistence**
   - Feast manages the generated features.
   - Online features are stored in Redis for low-latency inference.
   - Raw transaction events are persisted to S3/Parquet.
   - Historical data is stored in Snowflake/DuckDB for offline model training.

4. **Offline Model Training**
   - Point-in-time historical features are retrieved from the offline store.
   - Optuna is used for hyperparameter tuning.
   - LightGBM/CatBoost models are trained using the historical features.
   - The trained model and its artifacts are registered in MLflow.

5. **Real-Time Inference**
   - A client/payment gateway sends transaction details to the FastAPI service.
   - FastAPI retrieves the user's online features from Redis.
   - The production model generates the transaction risk probability.
   - A threshold-based decision produces an **Approve, Reject, or Challenge** response.

6. **Real-Time Observability**
   - Predictions and inference payloads are asynchronously logged to Kafka.
   - Evidently AI is used for drift detection.
   - Prometheus and Grafana provide monitoring dashboards and alerts.

### Architecture

                    ┌──────────────────────┐
                    │ CSV / Parquet Data   │
                    └──────────┬───────────┘
                               ↓
                    ┌──────────────────────┐
                    │ Python Event Producer│
                    └──────────┬───────────┘
                               ↓
                    ┌──────────────────────┐
                    │ Kafka / Redpanda     │
                    │ transaction-stream   │
                    └──────────┬───────────┘
                               ↓
                    ┌──────────────────────┐
                    │ Stream Processing    │
                    │ PySpark / Bytewax    │
                    └──────────┬───────────┘
                               ↓
                    ┌──────────────────────┐
                    │ Feast Feature Store  │
                    └───────┬───────┬──────┘
                            ↓       ↓
                         Redis    S3/Parquet
                            │       │
                            │    Snowflake/
                            │     DuckDB
                            │       │
                            │       ↓
                            │   ┌─────────────┐
                            │   │   Training  │
                            │   │   Optuna    │
                            │   │   LightGBM  │
                            │   │   CatBoost  │
                            │   └──────┬──────┘
                            │          ↓
                            │      MLflow
                            │          │
                            ↓          ↓
                       ┌─────────────────────┐
                       │       FastAPI       │
                       │ Real-Time Inference │
                       └──────────┬──────────┘
                                  ↓
                       ┌─────────────────────┐
                       │   Risk Prediction   │
                       │ P(Default)>Threshold│
                       └──────────┬──────────┘
                                  ↓
                       Approve / Reject /
                           Challenge
                                  │
                                  ↓
                       ┌─────────────────────┐
                       │ Kafka inference-log │
                       └──────────┬──────────┘
                                  ↓
                       ┌─────────────────────┐
                       │ Evidently AI        │
                       │ Drift Detection     │
                       └──────────┬──────────┘
                                  ↓
                       ┌─────────────────────┐
                       │ Prometheus + Grafana│
                       └─────────────────────┘
