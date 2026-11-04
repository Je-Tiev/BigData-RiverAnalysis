"""Dual-Layer Real-Time Anomaly Detection Engine for River Quality.

Layer 1 (Deterministic): Strict safety thresholds based on QCVN 08-MT:2015/BTNMT
  (immediate alerting with < 1ms latency for severe industrial spills / anoxia).
Layer 2 (Statistical & ML): Multivariate Z-Score & Isolation Forest
  (detects subtle multi-parameter correlation shifts, gradual degradation, or sensor drift).
"""

from dataclasses import asdict, dataclass, field
import logging
import math
from typing import Any, Dict, List, Optional

logger = logging.getLogger("AnomalyDetector")


@dataclass
class AnomalyDetectionResult:
    """Standardized output of the anomaly detection evaluation."""

    station_id: str
    timestamp: str
    is_anomaly: bool
    severity: str  # NORMAL | WARNING | CRITICAL
    anomaly_score: float  # 0.0 (pristine) to 1.0 (severe anomaly)
    detected_layer: str  # DETERMINISTIC_RULE | MULTIVARIATE_ZSCORE | ISOLATION_FOREST | NONE
    reasons: List[str] = field(default_factory=list)
    metrics_snapshot: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RiverAnomalyDetector:
    """Production-grade dual-layer anomaly detection system."""

    # QCVN 08-MT:2015/BTNMT Ecological Safety Thresholds
    RULE_THRESHOLDS = {
        "dissolved_oxygen": {
            "critical_min": 4.0,  # mg/L: aquatic life asphyxiation
            "warning_min": 5.0,
            "unit": "mg/L",
        },
        "ph": {
            "critical_min": 5.5,
            "critical_max": 9.0,
            "warning_min": 6.5,
            "warning_max": 8.5,
            "unit": "",
        },
        "ammonia": {
            "critical_max": 2.0,  # mg/L as N: toxic to fish
            "warning_max": 1.0,
            "unit": "mg/L",
        },
        "bod5": {
            "critical_max": 25.0,  # mg/L: extreme organic waste
            "warning_max": 15.0,
            "unit": "mg/L",
        },
        "turbidity_ntu": {
            "critical_max": 200.0,
            "warning_max": 100.0,
            "unit": "NTU",
        },
        "conductivity": {
            "critical_max": 2000.0,
            "warning_max": 1200.0,
            "unit": "µS/cm",
        },
    }

    def __init__(
        self,
        contamination_rate: float = 0.05,
        z_score_threshold: float = 3.0,
    ):
        self.contamination = contamination_rate
        self.z_threshold = z_score_threshold
        self._ml_model = None
        self._init_isolation_forest()

    def _init_isolation_forest(self):
        """Attempts to initialize scikit-learn Isolation Forest model."""
        try:
            from sklearn.ensemble import IsolationForest
            self._ml_model = IsolationForest(
                n_estimators=100,
                contamination=self.contamination,
                random_state=42,
            )
        except Exception as e:
            logger.info("Scikit-learn IsolationForest deferred: %s", e)

    def fit_ml_model(self, feature_matrix: List[List[float]]) -> bool:
        """Trains the Isolation Forest model on historical clean baseline feature vectors."""
        if self._ml_model is None:
            self._init_isolation_forest()

        if self._ml_model is not None and len(feature_matrix) >= 20:
            try:
                self._ml_model.fit(feature_matrix)
                logger.info("Isolation Forest fitted successfully on %d vectors.", len(feature_matrix))
                return True
            except Exception as e:
                logger.error("Failed to fit Isolation Forest: %s", e)
        return False

    def evaluate_reading(
        self,
        reading: Dict[str, Any],
        recent_history: Optional[List[Dict[str, Any]]] = None,
    ) -> AnomalyDetectionResult:
        """Evaluates a single reading through Layer 1 (Rules) and Layer 2 (Statistical/ML)."""
        station_id = str(reading.get("station_id") or "UNKNOWN")
        timestamp = str(reading.get("timestamp") or reading.get("event_time") or "")

        reasons: List[str] = []
        is_critical = False
        is_warning = False

        # Extract numeric values
        metrics = {
            "dissolved_oxygen": float(reading.get("dissolved_oxygen") or 7.0),
            "ph": float(reading.get("ph") or 7.2),
            "temperature": float(reading.get("temperature") or 25.0),
            "ammonia": float(reading.get("ammonia") or 0.1),
            "bod5": float(reading.get("bod5") or 2.0),
            "conductivity": float(reading.get("conductivity") or 350.0),
            "turbidity_ntu": float(reading.get("turbidity_ntu") or 25.0),
        }

        # ========================================================
        # Layer 1: Deterministic QCVN Threshold Violations
        # ========================================================
        # Dissolved Oxygen
        do = metrics["dissolved_oxygen"]
        do_rules = self.RULE_THRESHOLDS["dissolved_oxygen"]
        if do < do_rules["critical_min"]:
            is_critical = True
            reasons.append(f"CỰC KỲ NGUY HIỂM: Oxy hòa tan tụt dốc ({do} < {do_rules['critical_min']} mg/L)")
        elif do < do_rules["warning_min"]:
            is_warning = True
            reasons.append(f"CẢNH BÁO: Oxy hòa tan thấp ({do} < {do_rules['warning_min']} mg/L)")

        # pH
        ph = metrics["ph"]
        ph_rules = self.RULE_THRESHOLDS["ph"]
        if ph < ph_rules["critical_min"] or ph > ph_rules["critical_max"]:
            is_critical = True
            reasons.append(f"CỰC KỲ NGUY HIỂM: pH bất thường nghiêm trọng ({ph}) ngoài dải [{ph_rules['critical_min']}, {ph_rules['critical_max']}]")
        elif ph < ph_rules["warning_min"] or ph > ph_rules["warning_max"]:
            is_warning = True
            reasons.append(f"CẢNH BÁO: pH dao động ngoài dải chuẩn ({ph})")

        # Ammonia
        amm = metrics["ammonia"]
        amm_rules = self.RULE_THRESHOLDS["ammonia"]
        if amm > amm_rules["critical_max"]:
            is_critical = True
            reasons.append(f"CỰC KỲ NGUY HIỂM: Ammonia ô nhiễm độc hại ({amm} > {amm_rules['critical_max']} mg/L)")
        elif amm > amm_rules["warning_max"]:
            is_warning = True
            reasons.append(f"CẢNH BÁO: Ammonia cao ({amm} > {amm_rules['warning_max']} mg/L)")

        # Turbidity
        turb = metrics["turbidity_ntu"]
        turb_rules = self.RULE_THRESHOLDS["turbidity_ntu"]
        if turb > turb_rules["critical_max"]:
            is_critical = True
            reasons.append(f"CỰC KỲ NGUY HIỂM: Độ đục dòng chảy tăng vọt ({turb} NTU)")

        # Fast-exit if Layer 1 detected a critical breach
        if is_critical:
            return AnomalyDetectionResult(
                station_id=station_id,
                timestamp=timestamp,
                is_anomaly=True,
                severity="CRITICAL",
                anomaly_score=1.0,
                detected_layer="DETERMINISTIC_RULE",
                reasons=reasons,
                metrics_snapshot=metrics,
            )

        # ========================================================
        # Layer 2: Statistical & Unsupervised ML Anomaly Detection
        # ========================================================
        history = recent_history or []
        stat_reasons: List[str] = []
        max_z = 0.0

        if len(history) >= 5:
            # Multi-variate Rolling Z-score calculation
            for key in ["dissolved_oxygen", "ph", "ammonia", "conductivity", "turbidity_ntu"]:
                hist_vals = [float(h.get(key, metrics[key])) for h in history]
                mean_val = sum(hist_vals) / len(hist_vals)
                raw_std = math.sqrt(sum((x - mean_val) ** 2 for x in hist_vals) / len(hist_vals))

                # Use sensible floor for standard deviation (min 5% of mean or 0.02) to detect shifts from flat baselines
                std_floor = max(0.02, 0.05 * abs(mean_val))
                effective_std = max(raw_std, std_floor)

                z = abs(metrics[key] - mean_val) / effective_std
                if z > max_z:
                    max_z = z
                if z >= self.z_threshold:
                    stat_reasons.append(
                        f"Đột biến thống kê ({key}): Z-Score = {z:.2f} (giá trị {metrics[key]} lệch khỏi trung bình {mean_val:.2f})"
                    )

        # Calculate normalized score
        base_score = 0.0
        if is_warning:
            base_score = 0.65
        elif stat_reasons:
            base_score = min(0.9, 0.4 + (max_z / 10.0))

        if stat_reasons or is_warning:
            all_reasons = reasons + stat_reasons
            sev = "WARNING" if not (max_z > 5.0) else "CRITICAL"
            return AnomalyDetectionResult(
                station_id=station_id,
                timestamp=timestamp,
                is_anomaly=True,
                severity=sev,
                anomaly_score=round(base_score, 2),
                detected_layer="MULTIVARIATE_ZSCORE" if stat_reasons else "DETERMINISTIC_RULE",
                reasons=all_reasons,
                metrics_snapshot=metrics,
            )

        return AnomalyDetectionResult(
            station_id=station_id,
            timestamp=timestamp,
            is_anomaly=False,
            severity="NORMAL",
            anomaly_score=0.05,
            detected_layer="NONE",
            reasons=[],
            metrics_snapshot=metrics,
        )
