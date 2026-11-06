"""Comprehensive Test Suite for Phase 7: Advanced Hydrological Intelligence, Sensor Diagnostics & Resilient MLOps.

Verifies Acceptance Criteria:
- AC-04: Basin Graph, Streeter-Phelps DO Sag Curve, 1D ADR & Spatial Causal Attribution.
- AC-05: Emerson (1975) NH3 Kinetics, QCVN 08:2023 Classification & AATRI Index.
- AC-06: Sensor Diagnostics (Flatline, Drift, Spike), 1D Adaptive Kalman & Soft-Sensor Imputation.
- AC-07: Statistical Drift (PSI, KS-test), Adaptive Conformal Prediction (>= 93% coverage) & Champion vs Challenger.
- AC-08: DLQ Recovery Engine (>= 90% heal rate) & FastAPI Microservice Endpoints.
"""

import math
from datetime import UTC, datetime, timedelta

import numpy as np
import pytest
from fastapi.testclient import TestClient

from src.hydrology.propagation_engine import (
    AdvectionDispersionReactionModel,
    RiverBasinGraph,
    RiverReach,
    SpatialCausalAttributionEngine,
    StreeterPhelpsSagModel,
)
from src.ml.drift_and_conformal import (
    AdaptiveSplitConformalPredictor,
    ChampionChallengerEvaluator,
    HydrologicalDriftMonitor,
)
from src.quality.dlq_recovery import DLQRecoveryEngine
from src.quality.ecological_risk import (
    AquaticToxicityRiskEngine,
    EmersonAmmoniaKinetics,
    QCVN082023Classifier,
)
from src.quality.sensor_diagnostics import (
    AdaptiveKalmanFilter1D,
    SensorFaultDetector,
    SoftSensorImputer,
)
from src.serving.api import app

client = TestClient(app)


# ============================================================================
# AC-04: Basin Graph & Streeter-Phelps & 1D ADR & Spatial Causal Attribution
# ============================================================================
class TestAC04HydrologyAndPropagation:
    """Verifies river network topology, analytical sag curve, and causal attribution."""

    def test_river_basin_graph_topology_and_travel_times(self) -> None:
        graph = RiverBasinGraph(load_defaults=True)

        # Verify default topologies exist
        assert "ST_DA_HB_02" in graph.stations
        assert "ST_LO_VT_03" in graph.stations
        assert "ST_HONG_LB_01" in graph.stations
        assert "ST_SAIGON_BC_06" in graph.stations
        assert "ST_TIEN_MYTHO_08" in graph.stations

        # Travel time Da -> Confluence Ba Vi -> Long Bien
        # Da -> Ba Vi: 65km / (1.2 * 3.6) = 15.046h
        # Ba Vi -> Long Bien: 55km / (0.9 * 3.6) = 16.975h
        # Total approx 32.02 hours
        t_da_lb = graph.compute_path_travel_time("ST_DA_HB_02", "ST_HONG_LB_01")
        assert t_da_lb is not None
        assert 31.0 < t_da_lb < 33.0

        # Lo -> Long Bien approx 26.70 hours
        t_lo_lb = graph.compute_path_travel_time("ST_LO_VT_03", "ST_HONG_LB_01")
        assert t_lo_lb is not None
        assert 25.5 < t_lo_lb < 27.5

        # Disconnected basins return None
        assert graph.compute_path_travel_time("ST_DA_HB_02", "ST_SAIGON_BC_06") is None

        # Same station returns 0.0
        assert graph.compute_path_travel_time("ST_HONG_LB_01", "ST_HONG_LB_01") == 0.0

        # Ancestors of Long Bien must include Da, Lo, and Confluence
        ancestors = graph.get_all_upstream_ancestors("ST_HONG_LB_01")
        assert "ST_DA_HB_02" in ancestors
        assert "ST_LO_VT_03" in ancestors
        assert "ST_CONFLUENCE_BV" in ancestors

    def test_streeter_phelps_oxygen_sag_curve(self) -> None:
        model = StreeterPhelpsSagModel()

        # Check Benson-Krause saturation at 20°C (approx 9.08 - 9.1 mg/L)
        cs_20 = model.compute_saturation_do(20.0)
        assert 9.0 <= cs_20 <= 9.2

        # Simulate sag curve
        res = model.simulate_sag_curve(
            initial_bod=20.0,
            initial_do=7.5,
            temperature_c=25.0,
            flow_velocity_mps=1.0,
            max_travel_hours=72.0,
            step_hours=1.0,
        )

        assert res.critical_travel_time_hours > 0.0
        assert res.critical_distance_km > 0.0
        assert res.minimum_do_mg_l < 7.5
        assert res.maximum_deficit_mg_l > 0.0
        assert len(res.trajectory) == 73

        # Trajectory verifies DO sag and recovery
        do_initial = res.trajectory[0].dissolved_oxygen_mg_l
        assert math.isclose(do_initial, 7.5, abs_tol=0.1)

        # BOD decreases monotonically
        bod_values = [p.bod_remaining_mg_l for p in res.trajectory]
        for i in range(1, len(bod_values)):
            assert bod_values[i] <= bod_values[i - 1]

    def test_adr_contaminant_dispersion(self) -> None:
        reach = RiverReach(
            reach_id="REACH_TEST",
            upstream_station_id="ST_UP",
            downstream_station_id="ST_DOWN",
            distance_km=36.0,
            average_velocity_mps=1.0,  # 36000 m / 1 m/s = 36000s = 10.0 hours peak
            channel_width_m=100.0,
            channel_depth_m=4.0,
            longitudinal_dispersion_m2s=50.0,
        )
        adr = AdvectionDispersionReactionModel()

        sim_res = adr.simulate_pulse_propagation(
            spill_mass_kg=1000.0,
            reach=reach,
            duration_hours=24.0,
            step_hours=0.5,
        )

        # Theoretical peak arrival time = 36 km / (1 m/s * 3.6) = 10.0 hours
        assert math.isclose(sim_res.peak_arrival_time_hours, 10.0, abs_tol=0.01)
        assert sim_res.peak_concentration_mg_l > 0.0
        assert sim_res.plume_standard_deviation_km > 0.0

        # Spatial snapshot test
        spatial_profile = adr.simulate_spatial_profile(
            spill_mass_kg=1000.0,
            reach=reach,
            time_hours=10.0,
            num_points=50,
        )
        assert len(spatial_profile) == 51

    def test_spatial_causal_attribution_upstream_detected(self) -> None:
        graph = RiverBasinGraph(load_defaults=True)
        engine = SpatialCausalAttributionEngine()
        incident_time = datetime(2026, 11, 6, 14, 0, tzinfo=UTC)

        # Upstream Da station had an acute ammonia spike 32 hours prior
        t_travel = graph.compute_path_travel_time("ST_DA_HB_02", "ST_HONG_LB_01")
        assert t_travel is not None
        spike_time_up = incident_time - timedelta(hours=t_travel)

        downstream_series = [
            {"timestamp": incident_time.isoformat(), "ammonia": 6.8},
        ]
        upstream_series = {
            "ST_DA_HB_02": [
                {"timestamp": spike_time_up.isoformat(), "ammonia": 7.5},
            ],
            "ST_LO_VT_03": [
                {"timestamp": (incident_time - timedelta(hours=26.0)).isoformat(), "ammonia": 0.2},
            ],
        }

        report = engine.attribute_pollution_incident(
            downstream_station_id="ST_HONG_LB_01",
            incident_time=incident_time,
            pollutant_param="ammonia",
            downstream_series=downstream_series,
            upstream_series_by_station=upstream_series,
            graph=graph,
        )

        assert not report.probable_lateral_discharge_detected
        assert report.observed_peak_value == 6.8
        da_candidates = [c for c in report.candidate_attributions if c.station_id == "ST_DA_HB_02"]
        assert len(da_candidates) == 1
        assert da_candidates[0].is_probable_source
        assert da_candidates[0].confidence_level == "HIGH"

    def test_spatial_causal_attribution_lateral_illegal_spill(self) -> None:
        graph = RiverBasinGraph(load_defaults=True)
        engine = SpatialCausalAttributionEngine()
        incident_time = datetime(2026, 11, 6, 14, 0, tzinfo=UTC)

        # Downstream experiences huge peak, but ALL upstream stations are totally normal
        downstream_series = [
            {"timestamp": incident_time.isoformat(), "ammonia": 8.5},
        ]
        upstream_series = {
            "ST_DA_HB_02": [{"timestamp": incident_time.isoformat(), "ammonia": 0.1}],
            "ST_LO_VT_03": [{"timestamp": incident_time.isoformat(), "ammonia": 0.1}],
            "ST_CONFLUENCE_BV": [{"timestamp": incident_time.isoformat(), "ammonia": 0.1}],
        }

        report = engine.attribute_pollution_incident(
            downstream_station_id="ST_HONG_LB_01",
            incident_time=incident_time,
            pollutant_param="ammonia",
            downstream_series=downstream_series,
            upstream_series_by_station=upstream_series,
            graph=graph,
        )

        assert report.probable_lateral_discharge_detected
        assert report.unaccounted_residual_ratio > 0.6
        assert "xả thải lén" in report.diagnostic_summary


# ============================================================================
# AC-05: Emerson (1975) Ammonia Kinetics, QCVN 08:2023 & AATRI Index
# ============================================================================
class TestAC05EcologicalRiskAndQCVN:
    """Verifies Emerson aqueous ammonia speciation and QCVN 08:2023 classification."""

    def test_emerson_ammonia_kinetics_benchmarks(self) -> None:
        kinetics = EmersonAmmoniaKinetics()

        # Benchmark 1: At 25°C, pH 8.0 -> f_NH3 approx 5.38% (+- 0.2%)
        f_nh3_8 = kinetics.compute_free_fraction(ph=8.0, temperature_c=25.0)
        assert 0.0518 <= f_nh3_8 <= 0.0558

        # Benchmark 2: At 25°C, pH 7.0 -> f_NH3 approx 0.56% (+- 0.05%)
        f_nh3_7 = kinetics.compute_free_fraction(ph=7.0, temperature_c=25.0)
        assert 0.0051 <= f_nh3_7 <= 0.0061

        # Free ammonia calculation for 2.0 mg/L total ammonia at pH 8.0, 25°C
        free_ammonia = kinetics.compute_free_ammonia(ammonia_total=2.0, ph=8.0, temperature_c=25.0)
        assert math.isclose(free_ammonia, 2.0 * f_nh3_8, rel_tol=1e-5)

        # Toxicity level classification
        assert kinetics.evaluate_toxicity_level(0.01) == "SAFE"
        assert kinetics.evaluate_toxicity_level(0.03) == "CHRONIC"
        assert kinetics.evaluate_toxicity_level(0.12) == "SUBLETHAL"
        assert kinetics.evaluate_toxicity_level(0.25) == "ACUTE"

    def test_qcvn_08_2023_limiting_factor_classification(self) -> None:
        classifier = QCVN082023Classifier()

        # Test pristine water sample -> Tier A
        clean_reading = {
            "ph": 7.2,
            "dissolved_oxygen": 7.5,
            "ammonia": 0.15,
            "bod5": 2.5,
            "turbidity_ntu": 12.0,
            "chloride": 150.0,
        }
        res_a = classifier.classify_reading(clean_reading)
        assert res_a["overall_tier"] == "A"
        assert len(res_a["limiting_parameters"]) == 0

        # Test sample where only Ammonia exceeds (1.2 mg/L -> Tier D), all others Tier A
        polluted_reading = {
            "ph": 7.2,
            "dissolved_oxygen": 7.5,
            "ammonia": 1.2,  # > 1.0 -> Tier D
            "bod5": 3.0,
            "turbidity_ntu": 15.0,
        }
        res_d = classifier.classify_reading(polluted_reading)
        assert res_d["overall_tier"] == "D"
        assert "ammonia" in res_d["limiting_parameters"]

        # Test individual parameter boundary checks
        assert classifier.classify_parameter("ph", 6.5) == "A"
        assert classifier.classify_parameter("ph", 5.8) == "B"
        assert classifier.classify_parameter("ph", 5.2) == "C"
        assert classifier.classify_parameter("ph", 4.5) == "D"

        assert classifier.classify_parameter("do", 6.5) == "A"
        assert classifier.classify_parameter("do", 5.2) == "B"
        assert classifier.classify_parameter("do", 3.5) == "C"
        assert classifier.classify_parameter("do", 2.0) == "D"

    def test_aatri_acute_toxicity_risk_index(self) -> None:
        engine = AquaticToxicityRiskEngine()

        # Pristine sample -> LOW_RISK
        pristine = {"temperature": 24.0, "ph": 7.2, "dissolved_oxygen": 7.8, "ammonia": 0.05}
        r_pristine = engine.compute_aatri(pristine)
        assert r_pristine["aatri_score"] < 25.0
        assert r_pristine["risk_level"] == "LOW_RISK"

        # Hazardous sample with high ammonia + hypoxia + high pH -> CRITICAL_ACUTE_RISK
        hazardous = {"temperature": 30.0, "ph": 8.9, "dissolved_oxygen": 2.0, "ammonia": 4.5}
        r_haz = engine.compute_aatri(hazardous)
        assert r_haz["aatri_score"] >= 75.0
        assert r_haz["risk_level"] == "CRITICAL_ACUTE_RISK"
        assert r_haz["subscores"]["synergistic_interaction"] == 15.0


# ============================================================================
# AC-06: Sensor Diagnostics, Kalman Filter & Soft-Sensor Imputation
# ============================================================================
class TestAC06SensorDiagnosticsAndKalman:
    """Verifies sensor failure detection, adaptive Kalman filtering, and soft-sensor imputation."""

    def test_flatline_detection(self) -> None:
        detector = SensorFaultDetector()

        # Frozen series of 7 identical readings
        frozen_vals = [7.2, 7.2, 7.2, 7.2, 7.2, 7.2, 7.2]
        res = detector.detect_flatline(frozen_vals, min_repeats=5)
        assert res["is_flatline"]
        assert res["fault_type"] == "FLATLINE"
        assert res["max_consecutive_repeats"] == 7
        assert res["stuck_value"] == 7.2

        # Normal variable series
        normal_vals = [7.1, 7.3, 7.2, 7.4, 7.0, 7.5, 7.2]
        res_norm = detector.detect_flatline(normal_vals, min_repeats=5)
        assert not res_norm["is_flatline"]

    def test_drift_and_spike_detection(self) -> None:
        detector = SensorFaultDetector()

        # Steady drift: increasing by 0.1 every step
        drifting_vals = [7.0 + (0.1 * i) for i in range(15)]
        res_drift = detector.detect_drift(drifting_vals, max_slope_per_sample=0.04)
        assert res_drift["is_drift"]
        assert res_drift["fault_type"] == "CALIBRATION_DRIFT"

        # Transient spike at index 5
        series_with_spike = [7.1, 7.2, 7.1, 7.3, 7.2, 14.8, 7.2, 7.3, 7.1, 7.2]
        spikes = detector.detect_spikes(series_with_spike, threshold_mad=3.5)
        assert 5 in spikes

    def test_adaptive_kalman_filter_variance_reduction(self) -> None:
        np.random.seed(42)
        true_signal = 7.0
        raw_noisy = [float(true_signal + np.random.normal(0, 0.35)) for _ in range(80)]
        # Add transient spikes
        raw_noisy[20] = 12.5
        raw_noisy[50] = 1.5

        detector = SensorFaultDetector()
        detected_spikes = detector.detect_spikes(raw_noisy, threshold_mad=3.5)

        kf = AdaptiveKalmanFilter1D(q=0.01, r_base=0.1, r_spike_multiplier=1000.0)
        filtered = kf.filter_series(raw_noisy, spike_indices=detected_spikes)

        # Discard burn-in samples (first 10)
        raw_var = float(np.var(raw_noisy[10:]))
        filt_var = float(np.var(filtered[10:]))

        # AC-06 condition: variance reduced by > 50%
        var_reduction_pct = ((raw_var - filt_var) / raw_var) * 100.0
        assert var_reduction_pct > 50.0

        # Spike at index 20 suppressed significantly
        assert abs(filtered[20] - true_signal) < 1.0

    def test_soft_sensor_imputation(self) -> None:
        imputer = SoftSensorImputer()

        # Impute DO from temperature = 25.0 °C
        res_do = imputer.impute_missing_parameter(
            station_id="ST_HONG_LB_01",
            parameter="dissolved_oxygen",
            current_reading={"temperature": 25.0},
        )
        assert res_do["confidence_score"] > 0.70
        assert 6.0 <= res_do["imputed_value"] <= 8.5
        assert "BENSON_KRAUSE" in res_do["method"]

        # Impute Conductivity from Chloride and Hardness
        res_cond = imputer.impute_missing_parameter(
            station_id="ST_HONG_LB_01",
            parameter="conductivity",
            current_reading={"chloride": 120.0, "total_hardness": 80.0},
        )
        assert res_cond["confidence_score"] > 0.70
        assert res_cond["imputed_value"] == pytest.approx((2.1 * 120.0) + (1.8 * 80.0))

        # Spatial IDW imputation
        neighbors = [
            {"station_id": "ST_N1", "dissolved_oxygen": 7.4, "distance_km": 10.0},
            {"station_id": "ST_N2", "dissolved_oxygen": 7.1, "distance_km": 20.0},
        ]
        res_idw = imputer.impute_missing_parameter(
            station_id="ST_TARGET",
            parameter="dissolved_oxygen",
            current_reading={},
            neighbor_readings=neighbors,
        )
        assert res_idw["confidence_score"] > 0.70
        assert 7.1 <= res_idw["imputed_value"] <= 7.4


# ============================================================================
# AC-07: Statistical Drift, Conformal Prediction & Champion vs Challenger
# ============================================================================
class TestAC07DriftAndConformal:
    """Verifies PSI, Kolmogorov-Smirnov test, Conformal interval coverage, and model evaluator."""

    def test_psi_and_ks_test_drift(self) -> None:
        np.random.seed(42)
        ref = np.random.normal(loc=25.0, scale=1.0, size=500)

        # Same distribution -> PSI < 0.1 (STABLE)
        curr_stable = np.random.normal(loc=25.02, scale=1.01, size=500)
        psi_stable = HydrologicalDriftMonitor.compute_psi(ref, curr_stable)
        assert psi_stable < 0.10

        # Shifted distribution -> PSI >= 0.25 (SIGNIFICANT_DRIFT)
        curr_shifted = np.random.normal(loc=28.5, scale=1.8, size=500)
        psi_drift = HydrologicalDriftMonitor.compute_psi(ref, curr_shifted)
        assert psi_drift >= 0.25

        # Two-sample KS-test detects drift
        _, p_val, drift_flag = HydrologicalDriftMonitor.compute_ks_drift(ref, curr_shifted, alpha=0.05)
        assert drift_flag
        assert p_val < 0.01

    def test_adaptive_conformal_prediction_empirical_coverage(self) -> None:
        np.random.seed(123)
        predictor = AdaptiveSplitConformalPredictor()

        # Calibration set (n = 100)
        y_true_cal = np.random.normal(loc=7.5, scale=0.5, size=100)
        y_pred_cal = y_true_cal + np.random.normal(loc=0.0, scale=0.15, size=100)

        predictor.calibrate(list(y_true_cal), list(y_pred_cal), alpha=0.05)
        assert predictor.is_calibrated
        assert predictor.calibrated_q > 0.0

        # Independent test set (n = 200)
        y_true_test = np.random.normal(loc=7.5, scale=0.5, size=200)
        y_pred_test = y_true_test + np.random.normal(loc=0.0, scale=0.15, size=200)

        intervals = [predictor.predict_interval(pred, local_uncertainty=1.0) for pred in y_pred_test]

        coverage = AdaptiveSplitConformalPredictor.evaluate_empirical_coverage(list(y_true_test), intervals)
        # AC-07: Empirical coverage >= 93% (0.93)
        assert coverage >= 0.93

    def test_champion_challenger_evaluator(self) -> None:
        evaluator = ChampionChallengerEvaluator()

        y_true = [7.0, 7.2, 7.4, 7.1, 7.5, 7.3, 7.6, 7.2, 7.4, 7.5]
        # Champion has higher error
        y_champ = [7.5, 7.7, 7.9, 7.6, 8.0, 7.8, 8.1, 7.7, 7.9, 8.0]
        # Challenger is accurate
        y_chall = [7.05, 7.22, 7.38, 7.12, 7.49, 7.31, 7.58, 7.21, 7.41, 7.48]

        intervals_champ = [(c - 0.8, c + 0.8) for c in y_champ]
        intervals_chall = [(c - 0.3, c + 0.3) for c in y_chall]

        res = evaluator.evaluate_models(
            y_true=y_true,
            y_pred_champ=y_champ,
            y_pred_chall=y_chall,
            intervals_champ=intervals_champ,
            intervals_chall=intervals_chall,
        )

        assert res["decision"] == "PROMOTE"
        assert res["metrics"]["rmse_improvement_pct"] >= 5.0
        assert res["metrics"]["coverage_challenger"] >= 0.93


# ============================================================================
# AC-08: DLQ Recovery Engine & FastAPI Endpoints
# ============================================================================
class TestAC08DLQRecoveryAndFastAPI:
    """Verifies DLQ self-healing rules (>= 90% recovery) and new Phase 7 FastAPI REST endpoints."""

    def test_dlq_recovery_engine_rules(self) -> None:
        engine = DLQRecoveryEngine()

        # Test corrupted sample containing:
        # 1. String numbers with whitespace/noise
        # 2. Legacy schema keys
        # 3. Date missing time
        # 4. Out of bounds boundary values
        corrupted_batch = [
            {
                "StationID": "ST_HONG_LB_01",
                "Dissolved Oxygen": " 7.45 mg/L\n",
                "Water Temperature": "25.2",
                "pH": 14.04,  # Out of range, should clip to 14.0
                "timestamp": "2026-11-06",
            },
            {
                "station_id": "ST_DA_HB_02",
                "Ammonia-Total (as N)": " 0.35 ",
                "BOD - 5 days (Total)": "4.2",
                "timestamp": "2026-11-06 15:30:00",
                "dissolved_oxygen": -0.04,  # Negative, should clip to 0.0
            },
            {
                "stationid": "ST_LO_VT_03",
                "Conductivity @25°C": "420.5",
                "turbidity": "18.5",
            },
        ]

        healed_count = 0
        for sample in corrupted_batch:
            res = engine.heal_payload(sample)
            if res["healed"]:
                healed_count += 1
                repaired = res["repaired_payload"]
                assert "station_id" in repaired
                assert "timestamp" in repaired
                assert repaired["timestamp"].endswith("Z") or "+00:00" in repaired["timestamp"]

        # First sample assertions
        h1 = engine.heal_payload(corrupted_batch[0])["repaired_payload"]
        assert h1["dissolved_oxygen"] == 7.45
        assert h1["temperature"] == 25.2
        assert h1["ph"] == 14.0
        assert h1["is_clipped"]

        stats = engine.get_recovery_audit_stats()
        assert stats["heal_rate_pct"] >= 90.0

    def test_fastapi_propagation_endpoint(self) -> None:
        payload = {
            "upstream_station_id": "ST_DA_HB_02",
            "downstream_station_id": "ST_CONFLUENCE_BV",
            "initial_bod": 16.0,
            "initial_do": 7.2,
            "temperature_c": 26.0,
            "flow_velocity_mps": 1.2,
            "spill_mass_kg": 400.0,
            "duration_hours": 48.0,
        }
        response = client.post("/v1/hydrology/propagation", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "SUCCESS"
        assert "sag_curve" in data
        assert "adr_dispersion" in data
        assert data["sag_curve"]["minimum_do_mg_l"] > 0.0
        assert data["adr_dispersion"]["peak_arrival_time_hours"] > 0.0

    def test_fastapi_sensor_diagnose_endpoint(self) -> None:
        history = [
            {"timestamp": "2026-11-06T10:00:00Z", "dissolved_oxygen": 7.2, "temperature": 25.0},
            {"timestamp": "2026-11-06T10:15:00Z", "dissolved_oxygen": 7.3, "temperature": 25.1},
            {"timestamp": "2026-11-06T10:30:00Z", "dissolved_oxygen": 13.5, "temperature": 25.0},  # Spike
            {"timestamp": "2026-11-06T10:45:00Z", "dissolved_oxygen": 7.2, "temperature": 25.2},
        ]
        payload = {
            "station_id": "ST_HONG_LB_01",
            "history_records": history,
            "impute_missing": True,
        }
        response = client.post("/v1/sensor/diagnose", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["station_id"] == "ST_HONG_LB_01"
        assert "kalman_smoothed_do" in data
        assert len(data["kalman_smoothed_do"]) == len(history)

    def test_fastapi_drift_status_endpoints(self) -> None:
        # Test POST
        payload = {
            "station_id": "ST_HONG_LB_01",
            "features": ["temperature", "ph", "dissolved_oxygen"],
        }
        post_resp = client.post("/v1/ml/drift-status", json=payload)
        assert post_resp.status_code == 200
        post_data = post_resp.json()
        assert "overall_status" in post_data
        assert "conformal_guarantee" in post_data

        # Test GET
        get_resp = client.get("/v1/ml/drift-status?station_id=ST_HONG_LB_01")
        assert get_resp.status_code == 200
        get_data = get_resp.json()
        assert get_data["station_id"] == "ST_HONG_LB_01"
        assert "overall_status" in get_data

    def test_fastapi_dlq_heal_endpoint(self) -> None:
        payload = {
            "raw_message": {
                "StationID": "ST_TEST_01",
                "Dissolved Oxygen": " 7.82 mg/L",
                "timestamp": "2026-11-06",
            }
        }
        response = client.post("/v1/dlq/heal", json=payload)
        assert response.status_code == 200
        data = response.json()
        assert data["healed"]
        assert data["repaired_payload"]["dissolved_oxygen"] == 7.82
        assert data["repaired_payload"]["station_id"] == "ST_TEST_01"
        assert len(data["applied_fixes"]) > 0

    def test_fastapi_health_endpoint_loaded_models(self) -> None:
        response = client.get("/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] == "HEALTHY"
        assert "StreeterPhelpsSagModel_v1.3" in data["models_loaded"]
        assert "DLQRecoveryEngine_v1.3" in data["models_loaded"]
