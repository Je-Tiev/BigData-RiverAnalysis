"""Sensor Diagnostics, Fault Detection, Adaptive Kalman Filtering, and Soft-Sensor Imputation.

Implements:
1. SensorFaultDetector: Detects flatline stuck values, calibration drift, and transient spikes.
2. AdaptiveKalmanFilter1D: Discrete 1D Kalman filter with adaptive measurement covariance inflation.
3. SoftSensorImputer: Cross-variable physicochemical estimation and spatial IDW imputation.
"""

import logging
from typing import Any

import numpy as np

from src.hydrology.propagation_engine import StreeterPhelpsSagModel

logger = logging.getLogger("SensorDiagnostics")


class SensorFaultDetector:
    """Detects physical sensor degradation patterns: Flatline, Calibration Drift, and Spike Noise."""

    def detect_flatline(
        self,
        values: list[float],
        min_repeats: int = 5,
        var_threshold: float = 1e-5,
    ) -> dict[str, Any]:
        """Detects sensor flatline (zero variance or repeated frozen readings)."""
        if len(values) < min_repeats:
            return {
                "is_flatline": False,
                "fault_type": "NONE",
                "variance": 0.0,
                "max_consecutive_repeats": 0,
                "stuck_value": None,
            }

        arr = np.asarray(values, dtype=float)
        var = float(np.var(arr))

        # Check consecutive identical repeats
        max_repeats = 1
        curr_repeats = 1
        stuck_val: float | None = None

        for i in range(1, len(arr)):
            if abs(arr[i] - arr[i - 1]) < 1e-9:
                curr_repeats += 1
                if curr_repeats > max_repeats:
                    max_repeats = curr_repeats
                    stuck_val = float(arr[i])
            else:
                curr_repeats = 1

        is_flatline = (max_repeats >= min_repeats) or (var < var_threshold and len(arr) >= min_repeats)

        return {
            "is_flatline": bool(is_flatline),
            "fault_type": "FLATLINE" if is_flatline else "NONE",
            "variance": round(var, 7),
            "max_consecutive_repeats": max_repeats,
            "stuck_value": stuck_val if is_flatline else None,
        }

    def detect_drift(
        self,
        values: list[float],
        max_slope_per_sample: float = 0.04,
    ) -> dict[str, Any]:
        """Detects linear calibration drift (gradual systematic sensor offset over time)."""
        if len(values) < 5:
            return {
                "is_drift": False,
                "fault_type": "NONE",
                "slope": 0.0,
                "drift_magnitude": 0.0,
            }

        arr = np.asarray(values, dtype=float)
        n = len(arr)
        t = np.arange(n, dtype=float)

        t_mean = np.mean(t)
        arr_mean = np.mean(arr)

        denom = np.sum((t - t_mean) ** 2)
        if denom <= 0:
            return {"is_drift": False, "fault_type": "NONE", "slope": 0.0, "drift_magnitude": 0.0}

        slope = float(np.sum((t - t_mean) * (arr - arr_mean)) / denom)
        total_drift = abs(slope * (n - 1))

        # Monotonicity / linear correlation check
        corr = float(np.corrcoef(t, arr)[0, 1]) if np.std(arr) > 1e-6 else 0.0

        is_drift = abs(slope) >= max_slope_per_sample and abs(corr) >= 0.70

        return {
            "is_drift": bool(is_drift),
            "fault_type": "CALIBRATION_DRIFT" if is_drift else "NONE",
            "slope": round(slope, 5),
            "drift_magnitude": round(total_drift, 3),
            "correlation": round(corr, 3),
        }

    def detect_spikes(
        self,
        values: list[float],
        threshold_mad: float = 4.0,
    ) -> list[int]:
        """Detects transient artificial spike noise using Median Absolute Deviation (MAD)."""
        if len(values) < 3:
            return []

        arr = np.asarray(values, dtype=float)
        median_val = float(np.median(arr))
        abs_diff = np.abs(arr - median_val)
        mad = float(np.median(abs_diff))

        spike_indices: list[int] = []

        # If MAD is near zero, fallback to standard deviation or IQR
        denom = mad if mad > 1e-6 else float(np.std(arr))
        if denom < 1e-6:
            return []

        for i in range(len(arr)):
            # Modified Z-Score: M_i = 0.6745 * |x_i - median| / MAD
            m_score = 0.6745 * abs(arr[i] - median_val) / denom

            if m_score > threshold_mad:
                # Check transient property: neighbor samples return near each other
                is_transient = False
                if 0 < i < len(arr) - 1:
                    prev_v = arr[i - 1]
                    next_v = arr[i + 1]
                    curr_v = arr[i]
                    spike_height = abs(curr_v - prev_v)
                    neighbor_delta = abs(next_v - prev_v)
                    if spike_height > 1e-6 and neighbor_delta < 0.6 * spike_height:
                        is_transient = True
                elif i == 0 and len(arr) > 2:
                    if abs(arr[1] - arr[2]) < 0.5 * abs(arr[0] - arr[1]):
                        is_transient = True
                elif i == len(arr) - 1 and len(arr) > 2:
                    if abs(arr[-2] - arr[-3]) < 0.5 * abs(arr[-1] - arr[-2]):
                        is_transient = True

                if is_transient or m_score > (threshold_mad * 1.5):
                    spike_indices.append(i)

        return spike_indices

    def diagnose_station(self, history_records: list[dict[str, Any]]) -> dict[str, Any]:
        """Performs full diagnostic scan across all telemetry parameters for a station."""
        if not history_records:
            return {"status": "NO_DATA", "faults_detected": [], "clean_ratio": 1.0}

        parameters = ["dissolved_oxygen", "ph", "temperature", "conductivity", "ammonia"]
        diagnostics: dict[str, Any] = {}
        faults_found: list[dict[str, Any]] = []

        for param in parameters:
            vals = [float(r[param]) for r in history_records if param in r and r[param] is not None]
            if len(vals) < 3:
                continue

            flat = self.detect_flatline(vals)
            drift = self.detect_drift(vals)
            spikes = self.detect_spikes(vals)

            param_status = "HEALTHY"
            if flat["is_flatline"]:
                param_status = "FAULT_FLATLINE"
                faults_found.append({"parameter": param, "type": "FLATLINE", "details": flat})
            elif drift["is_drift"]:
                param_status = "FAULT_DRIFT"
                faults_found.append({"parameter": param, "type": "CALIBRATION_DRIFT", "details": drift})
            elif spikes:
                param_status = "FAULT_SPIKE"
                faults_found.append({"parameter": param, "type": "SPIKES", "count": len(spikes), "indices": spikes})

            diagnostics[param] = {
                "status": param_status,
                "flatline": flat,
                "drift": drift,
                "spike_indices": spikes,
            }

        clean_ratio = (len(parameters) - len(faults_found)) / max(1, len(parameters))

        return {
            "status": "DEGRADED" if faults_found else "HEALTHY",
            "faults_detected": faults_found,
            "clean_ratio": round(clean_ratio, 2),
            "diagnostics_by_param": diagnostics,
        }


class AdaptiveKalmanFilter1D:
    """Discrete 1D Adaptive Kalman Filter with measurement covariance inflation upon spike detection."""

    def __init__(
        self,
        q: float = 0.01,
        r_base: float = 0.1,
        r_spike_multiplier: float = 1000.0,
    ) -> None:
        """Initializes state and filter covariance parameters."""
        self.q = float(q)
        self.r_base = float(r_base)
        self.r_spike_multiplier = float(r_spike_multiplier)

        # Internal state
        self.x_hat: float = 0.0
        self.p: float = 1.0
        self._is_initialized: bool = False

    def step(self, measurement: float, is_spike: bool = False) -> tuple[float, float]:
        """Processes a single measurement step and returns (state_estimate, error_variance)."""
        z = float(measurement)

        if not self._is_initialized:
            self.x_hat = z
            self.p = self.r_base
            self._is_initialized = True
            return self.x_hat, self.p

        # 1. Prediction step: x_hat_minus = x_hat; P_minus = P + Q
        x_minus = self.x_hat
        p_minus = self.p + self.q

        # 2. Adaptive covariance inflation
        r_k = self.r_base * self.r_spike_multiplier if is_spike else self.r_base

        # 3. Kalman Gain update: K = P_minus / (P_minus + R_k)
        denom = p_minus + r_k
        k_gain = p_minus / denom if denom > 1e-9 else 0.0

        # 4. State estimate and covariance update
        self.x_hat = x_minus + (k_gain * (z - x_minus))
        self.p = (1.0 - k_gain) * p_minus

        return self.x_hat, self.p

    def filter_series(
        self,
        measurements: list[float],
        spike_indices: list[int] | None = None,
    ) -> list[float]:
        """Runs the adaptive Kalman filter across a full time-series sequence."""
        if not measurements:
            return []

        spike_set = set(spike_indices or [])
        self._is_initialized = False

        filtered: list[float] = []
        for i, val in enumerate(measurements):
            is_spike = i in spike_set
            est, _ = self.step(val, is_spike=is_spike)
            filtered.append(round(est, 4))

        return filtered


class SoftSensorImputer:
    """Physicochemical and spatial inverse-distance weighting (IDW) soft-sensor imputation."""

    def __init__(self) -> None:
        self.sag_model = StreeterPhelpsSagModel()

    def impute_missing_parameter(
        self,
        station_id: str,
        parameter: str,
        current_reading: dict[str, Any],
        neighbor_readings: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        """Imputes a missing parameter using physical modeling or spatial IDW."""
        p = parameter.lower().strip()

        # 1. Physicochemical Imputation: Dissolved Oxygen from Temperature
        if p in ("dissolved_oxygen", "do"):
            temp = float(current_reading.get("temperature", 25.0) or 25.0)
            sat_do = self.sag_model.compute_saturation_do(temp)
            # Typically unpolluted water holds ~85%-90% of saturation
            estimated_do = round(0.90 * sat_do, 2)
            return {
                "station_id": station_id,
                "parameter": parameter,
                "imputed_value": estimated_do,
                "confidence_score": 0.88,
                "method": "PHYSICAL_BENSON_KRAUSE_DO_EQUILIBRIUM",
                "notes": f"Estimated DO from water temperature {temp}°C via Benson-Krause formulation.",
            }

        # 2. Physicochemical Imputation: Conductivity from Chloride & Hardness
        if p in ("conductivity", "ec"):
            chloride = current_reading.get("chloride")
            hardness = current_reading.get("total_hardness")
            if chloride is not None and hardness is not None:
                est_cond = (2.1 * float(chloride)) + (1.8 * float(hardness))
                return {
                    "station_id": station_id,
                    "parameter": parameter,
                    "imputed_value": round(est_cond, 1),
                    "confidence_score": 0.84,
                    "method": "STOICHIOMETRIC_IONIC_BALANCE",
                    "notes": "Estimated conductivity from Chloride and Total Hardness ionic contributions.",
                }
            if chloride is not None:
                est_cond = 2.8 * float(chloride)
                return {
                    "station_id": station_id,
                    "parameter": parameter,
                    "imputed_value": round(est_cond, 1),
                    "confidence_score": 0.75,
                    "method": "UNIVARIATE_CHLORIDE_CORRELATION",
                    "notes": "Estimated conductivity from Chloride concentration.",
                }

        # 3. Spatial IDW Imputation across neighbor stations
        if neighbor_readings:
            valid_vals = []
            weights = []
            for n_read in neighbor_readings:
                val = n_read.get(parameter)
                if val is not None:
                    # Default unit distance if spatial coords are absent
                    dist = float(n_read.get("distance_km", 20.0) or 20.0)
                    w = 1.0 / (max(1.0, dist) ** 2)
                    valid_vals.append(float(val))
                    weights.append(w)

            if valid_vals:
                weights_norm = np.array(weights) / sum(weights)
                imputed = float(np.sum(np.array(valid_vals) * weights_norm))
                return {
                    "station_id": station_id,
                    "parameter": parameter,
                    "imputed_value": round(imputed, 2),
                    "confidence_score": 0.82,
                    "method": "SPATIAL_INVERSE_DISTANCE_WEIGHTING",
                    "notes": f"Spatial interpolation from {len(valid_vals)} basin neighbor stations.",
                }

        # Default fallback
        fallback_val = float(current_reading.get(parameter, 0.0) or 0.0)
        return {
            "station_id": station_id,
            "parameter": parameter,
            "imputed_value": fallback_val,
            "confidence_score": 0.50,
            "method": "HISTORICAL_MEAN_FALLBACK",
            "notes": "Fallback default imputation.",
        }
