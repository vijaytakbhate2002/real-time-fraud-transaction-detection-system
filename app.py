"""Minimal API for the MLflow fraud champion model."""

import os

import dagshub
import mlflow.sklearn
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel


MODEL_URI = os.getenv("MLFLOW_MODEL_URI", "models:/FraudDetectionModel@champion")
model = None
app = FastAPI(title="Fraud Detection API")


class PredictionRequest(BaseModel):
    features: dict


def load_model():
    global model
    if model is None:
        dagshub.init(
            repo_owner=os.getenv("DAGSHUB_REPO_OWNER", "vijaytakbhate2002"),
            repo_name=os.getenv(
                "DAGSHUB_REPO_NAME", "credit-card-fraud-transaction-model"
            ),
            mlflow=True,
        )
        model = mlflow.sklearn.load_model(MODEL_URI)
    return model

try:
    champion = load_model()
except Exception as exc:
    raise HTTPException(
        status_code=503,
        detail="Could not load the MLflow champion model. Check DagsHub connectivity and credentials.",
    ) from exc

@app.get("/health")
def health():
    return {"status": "ok", "model_loaded": model is not None, "model_url": MODEL_URI}


@app.post("/predict")
def predict(request: PredictionRequest):
    transaction = pd.DataFrame([request.features])
    probability = float(champion.predict_proba(transaction)[0][1])
    prediction = int(probability >= 0.5)
    return {
        "prediction": prediction,
        "label": "fraud" if prediction else "legitimate",
        "fraud_probability": probability,
        "model_url": MODEL_URI,
    }
