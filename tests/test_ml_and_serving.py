"""Unit and Integration Tests for Machine Learning, Forecaster, and FastAPI Serving.

Tests Feature Extractor, Dual-layer Anomaly Detector, Forecaster with Confidence
Intervals, MLflow Tracking, Alert Manager, and FastAPI Endpoints.
"""

from datetime import datetime, timezone
import json
import os
import sys
import unittest

from fastapi.testclient import TestClient

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.ml.alert_manager import RiverAlertManager
from src.ml.anomaly_detector import RiverAnomalyDetector
from src.ml.feature_extractor import FEATURE_COLUMNS, RiverFeatureExtractor
from src.ml.forecaster import RiverWaterQualityForecaster
from src.ml.tracking import RiverMLflowTracker
from src.serving.api import app


class TestRiverFeatureExtractor(unittest.TestCase):
    """Test suite for feature extraction and temporal encodings."""

    def setUp(self):
        self.extractor = RiverFeatureExtractor()
        self.sample_reading = {
            "station_id": "ST_HONG_01",
            "timestamp": "2026-11-04T12:00:00Z",
            "temperature": 25.0,
            "ph": 7.4,
            "dissolved_oxygen": 7.8,
            "conductivity": 380.0,
            "ammonia": 0.12,
            "turbidity_ntu": 35.0,
        }

    def test_cyclical_encodings_within_bounds(self):
        dt = datetime(2026, 6, 15, 12, 0, 0, tzinfo=timezone.utc)
        cyclical = self.extractor.extract_cyclical_time_features(dt)
        for key in ["hour_sin", "hour_cos", "month_sin", "month_cos"]:
            self.assertGreaterEqual(cyclical[key], -1.0)
            self.assertLessEqual(cyclical[key], 1.0)

    def test_feature_vector_contains_all_columns(self):
        feat_dict = self.extractor.transform_single_with_history(self.sample_reading, [])
        for col_name in FEATURE_COLUMNS:
            self.assertIn(col_name, feat_dict, f"Missing feature: {col_name}")

        vector = self.extractor.to_feature_vector(feat_dict)
        self.assertEqual(len(vector), len(FEATURE_COLUMNS))


class TestRiverAnomalyDetector(unittest.TestCase):
    """Test suite for dual-layer anomaly detection logic."""

    def setUp(self):
        self.detector = RiverAnomalyDetector(contamination_rate=0.05)
        self.normal_reading = {
            "station_id": "ST_NORMAL",
            "timestamp": "2026-11-04T12:00:00Z",
            "temperature": 24.5,
            "ph": 7.3,
            "dissolved_oxygen": 7.5,
            "ammonia": 0.08,
            "turbidity_ntu": 20.0,
            "conductivity": 320.0,
        }

    def test_normal_reading_returns_normal_severity(self):
        res = self.detector.evaluate_reading(self.normal_reading)
        self.assertFalse(res.is_anomaly)
        self.assertEqual(res.severity, "NORMAL")
        self.assertEqual(res.detected_layer, "NONE")

    def test_low_do_triggers_layer1_critical_anoxia(self):
        critical_reading = dict(self.normal_reading)
        critical_reading["dissolved_oxygen"] = 2.1  # Severe anoxia (< 4.0 mg/L)
        res = self.detector.evaluate_reading(critical_reading)

        self.assertTrue(res.is_anomaly)
        self.assertEqual(res.severity, "CRITICAL")
        self.assertEqual(res.detected_layer, "DETERMINISTIC_RULE")
        self.assertTrue(any("Oxy hòa tan" in r for r in res.reasons))

    def test_acid_discharge_triggers_critical(self):
        acid_reading = dict(self.normal_reading)
        acid_reading["ph"] = 4.2  # Severe acid spill (< 5.5)
        res = self.detector.evaluate_reading(acid_reading)

        self.assertTrue(res.is_anomaly)
        self.assertEqual(res.severity, "CRITICAL")
        self.assertTrue(any("pH" in r for r in res.reasons))

    def test_multivariate_zscore_triggers_warning_on_abrupt_shift(self):
        history = [dict(self.normal_reading) for _ in range(10)]
        sudden_shift = dict(self.normal_reading)
        sudden_shift["ammonia"] = 0.85  # Normal absolute value but 10x higher than baseline 0.08

        res = self.detector.evaluate_reading(sudden_shift, recent_history=history)
        self.assertTrue(res.is_anomaly)
        self.assertEqual(res.detected_layer, "MULTIVARIATE_ZSCORE")


class TestRiverAlertManager(unittest.TestCase):
    """Test suite for alert generation and packaging."""

    def setUp(self):
        self.alert_mgr = RiverAlertManager(kafka_bootstrap="mock:9092")

    def test_create_critical_alert_structure(self):
        alert = self.alert_mgr.create_alert(
            station_id="ST_ALERT_01",
            station_name="Trạm Test Cảnh Báo",
            river_name="Sông Sài Gòn",
            timestamp="2026-11-04T12:00:00Z",
            severity="CRITICAL",
            reasons=["Oxy hòa tan tụt dốc dưới 4.0 mg/L"],
            metrics={"dissolved_oxygen": 2.5, "ph": 7.1},
        )
        self.assertEqual(alert.severity, "CRITICAL")
        self.assertIn("sục khí", alert.recommended_action)
        self.assertTrue(len(alert.alert_id) > 10)


class TestRiverForecaster(unittest.TestCase):
    """Test suite for multi-step ahead trajectory forecasting."""

    def setUp(self):
        self.forecaster = RiverWaterQualityForecaster()
        self.reading = {
            "station_id": "ST_FORECAST_01",
            "river_name": "Sông Đồng Nai",
            "timestamp": "2026-11-04T08:00:00Z",
            "temperature": 26.0,
            "ph": 7.3,
            "dissolved_oxygen": 7.2,
            "ammonia": 0.15,
            "conductivity": 420.0,
            "turbidity_ntu": 30.0,
        }

    def test_forecast_generates_24_future_points(self):
        res = self.forecaster.forecast_trajectory(
            station_id="ST_FORECAST_01",
            river_name="Sông Đồng Nai",
            current_reading=self.reading,
            horizon_hours=24,
        )
        self.assertEqual(res.horizon_hours, 24)
        self.assertEqual(len(res.forecast_points), 24)

        for pt in res.forecast_points:
            self.assertLessEqual(pt.do_lower_95, pt.predicted_do)
            self.assertGreaterEqual(pt.do_upper_95, pt.predicted_do)
            self.assertGreaterEqual(pt.predicted_wqi, 0.0)
            self.assertLessEqual(pt.predicted_wqi, 100.0)
            self.assertIn(pt.predicted_label, ["Excellent", "Good", "Fair", "Marginal", "Poor"])


class TestRiverMLflowTracker(unittest.TestCase):
    """Test suite for MLflow tracking metadata management."""

    def setUp(self):
        self.tracker = RiverMLflowTracker(experiment_name="Test_Tracking")

    def test_log_run_and_promote(self):
        meta = self.tracker.log_training_run(
            model_name="TestGBM",
            parameters={"lr": 0.05, "depth": 4},
            metrics={"rmse": 0.28, "mae": 0.19},
        )
        self.assertEqual(meta.status, "COMPLETED")
        self.assertEqual(meta.stage, "Staging")

        promoted = self.tracker.promote_to_production(meta.run_id)
        self.assertTrue(promoted)


class TestFastAPIServing(unittest.TestCase):
    """Test suite for FastAPI model serving REST endpoints."""

    def setUp(self):
        self.client = TestClient(app)

    def test_health_endpoint(self):
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["status"], "HEALTHY")
        self.assertIn("RiverAnomalyDetector_v1.2", data["models_loaded"])

    def test_anomaly_score_endpoint(self):
        payload = {
            "reading": {
                "station_id": "ST_TEST_API",
                "temperature": 25.0,
                "ph": 7.4,
                "dissolved_oxygen": 7.8,
                "ammonia": 0.05,
            },
            "recent_history": [],
            "publish_alert_on_critical": False,
        }
        response = self.client.post("/v1/anomaly/score", json=payload)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["severity"], "NORMAL")
        self.assertFalse(data["is_anomaly"])

    def test_forecast_predict_endpoint(self):
        payload = {
            "station_id": "ST_HONG_LB_01",
            "river_name": "Sông Hồng",
            "current_reading": {
                "station_id": "ST_HONG_LB_01",
                "temperature": 24.5,
                "ph": 7.3,
                "dissolved_oxygen": 7.5,
            },
            "horizon_hours": 12,
        }
        response = self.client.post("/v1/predict/forecast", json=payload)
        self.assertEqual(response.status_code, 200)
        data = response.json()
        self.assertEqual(data["horizon_hours"], 12)
        self.assertEqual(len(data["forecast_points"]), 12)


if __name__ == "__main__":
    unittest.main()
