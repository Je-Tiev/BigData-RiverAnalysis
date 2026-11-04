"""Production FastAPI Model Serving Microservice for River Quality.

Provides RESTful endpoints for:
1. /v1/predict/forecast: 24h-72h multi-step forward predictions with 95% confidence bounds.
2. /v1/anomaly/score: Dual-layer real-time anomaly evaluation and emergency alert recommendations.
3. /health: Service readiness and model status checks.
"""

from datetime import datetime, timezone
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel, Field

# Ensure src modules are resolvable
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.contracts.river_schema import REFERENCE_STATIONS
from src.ml.alert_manager import RiverAlertManager
from src.ml.anomaly_detector import AnomalyDetectionResult, RiverAnomalyDetector
from src.ml.forecaster import RiverWaterQualityForecaster, StationForecastResult

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [ModelServingAPI]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("ModelServingAPI")

# FastAPI App Instance
app = FastAPI(
    title="River Water Quality ML Serving API",
    description="Production-grade API for hydrological forecasting and real-time anomaly detection.",
    version="1.2.0",
)

# Shared Model Singletons
anomaly_detector = RiverAnomalyDetector()
forecaster = RiverWaterQualityForecaster()
alert_manager = RiverAlertManager()

START_TIME = time.time()
REQUEST_COUNTER = {"forecast": 0, "anomaly": 0}


# ========================================================
# Pydantic Request & Response Schemas
# ========================================================
class SensorReadingInput(BaseModel):
    station_id: str = Field(..., json_schema_extra={"example": "ST_HONG_LB_01"})
    station_name: Optional[str] = Field(None, json_schema_extra={"example": "Trạm Cầu Long Biên"})
    river_name: Optional[str] = Field(None, json_schema_extra={"example": "Sông Hồng"})
    timestamp: Optional[str] = Field(None, json_schema_extra={"example": "2026-11-04T12:00:00Z"})
    temperature: float = Field(25.0, ge=-5.0, le=55.0, json_schema_extra={"example": 24.5})
    ph: float = Field(7.2, ge=0.0, le=14.0, json_schema_extra={"example": 7.3})
    dissolved_oxygen: float = Field(7.5, ge=0.0, le=30.0, json_schema_extra={"example": 7.8})
    conductivity: Optional[float] = Field(350.0, ge=0.0, json_schema_extra={"example": 380.0})
    ammonia: Optional[float] = Field(0.1, ge=0.0, json_schema_extra={"example": 0.12})
    bod5: Optional[float] = Field(2.0, ge=0.0, json_schema_extra={"example": 2.5})
    turbidity_ntu: Optional[float] = Field(25.0, ge=0.0, json_schema_extra={"example": 45.0})
    flow_rate_m3s: Optional[float] = Field(1200.0, ge=0.0, json_schema_extra={"example": 3200.0})


class ForecastRequest(BaseModel):
    station_id: str = Field(..., json_schema_extra={"example": "ST_HONG_LB_01"})
    river_name: Optional[str] = Field("Sông Hồng", json_schema_extra={"example": "Sông Hồng"})
    current_reading: SensorReadingInput
    recent_history: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    horizon_hours: int = Field(24, ge=1, le=72, json_schema_extra={"example": 24})


class AnomalyScoreRequest(BaseModel):
    reading: SensorReadingInput
    recent_history: Optional[List[Dict[str, Any]]] = Field(default_factory=list)
    publish_alert_on_critical: bool = Field(True, json_schema_extra={"example": True})


class HealthResponse(BaseModel):
    status: str
    version: str
    uptime_seconds: float
    models_loaded: List[str]
    total_predictions: Dict[str, int]


# ========================================================
# API Endpoints
# ========================================================
@app.get("/health", response_model=HealthResponse)
def get_health():
    """Healthcheck and model readiness endpoint."""
    return HealthResponse(
        status="HEALTHY",
        version="1.2.0",
        uptime_seconds=round(time.time() - START_TIME, 1),
        models_loaded=["RiverAnomalyDetector_v1.2", "HistGradientBoostingForecaster_v1.2"],
        total_predictions=REQUEST_COUNTER,
    )


@app.post("/v1/anomaly/score", response_model=Dict[str, Any])
def score_anomaly(request: AnomalyScoreRequest):
    """Scores a single sensor reading through the dual-layer anomaly detection engine."""
    REQUEST_COUNTER["anomaly"] += 1
    reading_dict = request.reading.model_dump()

    result: AnomalyDetectionResult = anomaly_detector.evaluate_reading(
        reading=reading_dict,
        recent_history=request.recent_history,
    )

    # If critical, optionally dispatch emergency alarm
    if result.severity == "CRITICAL" and request.publish_alert_on_critical:
        alert = alert_manager.create_alert(
            station_id=result.station_id,
            station_name=request.reading.station_name or result.station_id,
            river_name=request.reading.river_name or "Unknown River",
            timestamp=result.timestamp or datetime.now(timezone.utc).isoformat(),
            severity=result.severity,
            reasons=result.reasons,
            metrics=result.metrics_snapshot,
        )
        alert_manager.dispatch_alert(alert)

    return result.to_dict()


@app.post("/v1/predict/forecast", response_model=Dict[str, Any])
def predict_forecast(request: ForecastRequest):
    """Generates multi-step ahead forward forecasts (24h-72h) with 95% confidence intervals."""
    REQUEST_COUNTER["forecast"] += 1
    reading_dict = request.current_reading.model_dump()

    forecast_res: StationForecastResult = forecaster.forecast_trajectory(
        station_id=request.station_id,
        river_name=request.river_name or "Unknown River",
        current_reading=reading_dict,
        recent_history=request.recent_history,
        horizon_hours=request.horizon_hours,
    )

    return forecast_res.to_dict()


if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", "8000"))
    logger.info("Starting FastAPI serving engine on port %d...", port)
    uvicorn.run("src.serving.api:app", host="0.0.0.0", port=port, reload=False)
