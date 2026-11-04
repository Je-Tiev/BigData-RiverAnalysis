"""Feature Engineering & Feature Store Extractor for River Water Quality ML.

Transforms raw and Gold time-series records into feature matrices ready for:
1. Multi-step forecasting (LightGBM/HistGradientBoosting).
2. Unsupervised multi-variate anomaly detection (Isolation Forest / Z-Score).
"""

from datetime import datetime, timezone
import math
from typing import Any, Dict, List, Optional, Tuple

FEATURE_COLUMNS = [
    # Instantaneous Sensors
    "temperature",
    "ph",
    "dissolved_oxygen",
    "conductivity",
    "ammonia",
    "turbidity_ntu",
    # Cyclical Temporal Encodings
    "hour_sin",
    "hour_cos",
    "month_sin",
    "month_cos",
    # Lags (Autoregressive features)
    "do_lag_1",
    "do_lag_2",
    "do_lag_3",
    "ph_lag_1",
    "ph_lag_2",
    "amm_lag_1",
    # Rolling Statistics
    "do_rolling_mean_3",
    "do_rolling_std_3",
    "ph_rolling_mean_3",
    # Rate of Change (Deltas)
    "delta_do",
    "delta_ph",
    "delta_ammonia",
]


class RiverFeatureExtractor:
    """Transforms hydrological time-series into feature vectors with temporal context."""

    @staticmethod
    def extract_cyclical_time_features(dt: datetime) -> Dict[str, float]:
        """Calculates sine and cosine cyclical encodings for hour-of-day and month-of-year."""
        hour = dt.hour + (dt.minute / 60.0)
        month = dt.month

        pi2 = 2.0 * math.pi
        return {
            "hour_sin": round(math.sin(hour * pi2 / 24.0), 5),
            "hour_cos": round(math.cos(hour * pi2 / 24.0), 5),
            "month_sin": round(math.sin(month * pi2 / 12.0), 5),
            "month_cos": round(math.cos(month * pi2 / 12.0), 5),
        }

    @classmethod
    def transform_single_with_history(
        cls,
        current_reading: Dict[str, Any],
        recent_history: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, float]:
        """Extracts the full feature dictionary for a single reading given recent historical readings."""
        history = recent_history or []

        # Parse timestamp
        ts_str = str(current_reading.get("timestamp") or current_reading.get("event_time") or "")
        try:
            ts_clean = ts_str.replace("Z", "+00:00") if ts_str.endswith("Z") else ts_str
            dt = datetime.fromisoformat(ts_clean)
        except Exception:
            dt = datetime.now(timezone.utc)

        # 1. Base Sensor Values
        cur_do = float(current_reading.get("dissolved_oxygen", 7.5))
        cur_ph = float(current_reading.get("ph", 7.2))
        cur_temp = float(current_reading.get("temperature", 24.0))
        cur_cond = float(current_reading.get("conductivity", 350.0))
        cur_amm = float(current_reading.get("ammonia", 0.1))
        cur_turb = float(current_reading.get("turbidity_ntu", 25.0))

        # 2. Temporal Encodings
        time_feats = cls.extract_cyclical_time_features(dt)

        # 3. Lags
        # History is ordered from oldest to newest
        h_len = len(history)
        do_lag_1 = float(history[-1].get("dissolved_oxygen", cur_do)) if h_len >= 1 else cur_do
        do_lag_2 = float(history[-2].get("dissolved_oxygen", do_lag_1)) if h_len >= 2 else do_lag_1
        do_lag_3 = float(history[-3].get("dissolved_oxygen", do_lag_2)) if h_len >= 3 else do_lag_2

        ph_lag_1 = float(history[-1].get("ph", cur_ph)) if h_len >= 1 else cur_ph
        ph_lag_2 = float(history[-2].get("ph", ph_lag_1)) if h_len >= 2 else ph_lag_1

        amm_lag_1 = float(history[-1].get("ammonia", cur_amm)) if h_len >= 1 else cur_amm

        # 4. Rolling Statistics (window size = 3 including current)
        window_do = [cur_do] + [float(x.get("dissolved_oxygen", cur_do)) for x in history[-2:]]
        do_rolling_mean = sum(window_do) / len(window_do)
        if len(window_do) > 1:
            variance = sum((x - do_rolling_mean) ** 2 for x in window_do) / (len(window_do) - 1)
            do_rolling_std = math.sqrt(variance)
        else:
            do_rolling_std = 0.0

        window_ph = [cur_ph] + [float(x.get("ph", cur_ph)) for x in history[-2:]]
        ph_rolling_mean = sum(window_ph) / len(window_ph)

        # 5. Deltas
        delta_do = cur_do - do_lag_1
        delta_ph = cur_ph - ph_lag_1
        delta_amm = cur_amm - amm_lag_1

        return {
            "temperature": cur_temp,
            "ph": cur_ph,
            "dissolved_oxygen": cur_do,
            "conductivity": cur_cond,
            "ammonia": cur_amm,
            "turbidity_ntu": cur_turb,
            "hour_sin": time_feats["hour_sin"],
            "hour_cos": time_feats["hour_cos"],
            "month_sin": time_feats["month_sin"],
            "month_cos": time_feats["month_cos"],
            "do_lag_1": do_lag_1,
            "do_lag_2": do_lag_2,
            "do_lag_3": do_lag_3,
            "ph_lag_1": ph_lag_1,
            "ph_lag_2": ph_lag_2,
            "amm_lag_1": amm_lag_1,
            "do_rolling_mean_3": round(do_rolling_mean, 3),
            "do_rolling_std_3": round(do_rolling_std, 4),
            "ph_rolling_mean_3": round(ph_rolling_mean, 3),
            "delta_do": round(delta_do, 3),
            "delta_ph": round(delta_ph, 3),
            "delta_ammonia": round(delta_amm, 4),
        }

    @classmethod
    def transform_series(
        cls,
        series: List[Dict[str, Any]],
    ) -> List[Dict[str, float]]:
        """Transforms a full chronological series into an aligned list of feature dictionaries."""
        features_list = []
        for i, reading in enumerate(series):
            history = series[max(0, i - 5):i]
            features_list.append(cls.transform_single_with_history(reading, history))
        return features_list

    @classmethod
    def to_feature_vector(cls, feature_dict: Dict[str, float]) -> List[float]:
        """Extracts ordered list of floats conforming strictly to FEATURE_COLUMNS."""
        return [float(feature_dict.get(col, 0.0)) for col in FEATURE_COLUMNS]
