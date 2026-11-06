"""Unit Tests for Medallion Lakehouse Transformations & ML Feature Engineering.

Validates the logical transformations of Bronze, Silver, Gold Hourly WQI,
and ML Feature Store generation without requiring a live Spark cluster.
"""

import json
import math
import os
import sys
import unittest
from datetime import UTC, datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class MedallionLogicSimulator:
    """Simulates the relational transformations of Bronze, Silver, and Gold layers."""

    @staticmethod
    def bronze_transform(raw_key: str, raw_payload: str, offset: int = 101) -> dict:
        now = datetime.now(UTC)
        return {
            "kafka_key": raw_key,
            "raw_payload": raw_payload,
            "kafka_offset": offset,
            "ingest_timestamp": now.isoformat(),
            "ingest_date": now.strftime("%Y-%m-%d"),
        }

    @staticmethod
    def silver_transform_record(bronze_row: dict) -> tuple[bool, dict]:
        """Simulates Silver parsing, normalization, bounds check, and deduplication."""
        try:
            parsed = json.loads(bronze_row["raw_payload"])
        except (json.JSONDecodeError, TypeError):
            return False, {}

        station_id = parsed.get("station_id") or parsed.get("WaterbodyName")
        if not station_id:
            return False, {}

        event_ts = parsed.get("timestamp") or parsed.get("FullDate")
        if not event_ts:
            return False, {}

        # Physical bounds
        ph = float(parsed.get("ph", 7.0))
        do = float(parsed.get("dissolved_oxygen", 7.0))
        temp = float(parsed.get("temperature", 25.0))
        amm = float(parsed.get("ammonia", 0.1))

        if not (0.0 <= ph <= 14.0):
            return False, {}
        if not (0.0 <= do <= 30.0):
            return False, {}
        if not (-5.0 <= temp <= 55.0):
            return False, {}

        # Parse date components
        ts_clean = event_ts.replace("Z", "+00:00") if "Z" in event_ts else event_ts
        try:
            dt = datetime.fromisoformat(ts_clean)
        except (ValueError, TypeError):
            dt = datetime.strptime(event_ts, "%Y-%m-%d").replace(tzinfo=UTC)

        return True, {
            "station_id": station_id,
            "river_name": parsed.get("river_name", station_id),
            "event_time": dt.isoformat(),
            "year": dt.year,
            "month": dt.month,
            "temperature": temp,
            "ph": ph,
            "dissolved_oxygen": do,
            "ammonia": amm,
            "conductivity": float(parsed.get("conductivity", 300.0)),
            "turbidity_ntu": float(parsed.get("turbidity_ntu", 20.0)),
            "ingest_timestamp": bronze_row.get("ingest_timestamp"),
        }

    @staticmethod
    def compute_gold_hourly(silver_records: list) -> list:
        """Groups silver records by station and calculates hourly averages & CCME WQI."""
        grouped = {}
        for r in silver_records:
            st = r["station_id"]
            if st not in grouped:
                grouped[st] = []
            grouped[st].append(r)

        gold_records = []
        for st, items in grouped.items():
            count = len(items)
            avg_temp = sum(x["temperature"] for x in items) / count
            avg_ph = sum(x["ph"] for x in items) / count
            avg_do = sum(x["dissolved_oxygen"] for x in items) / count
            avg_amm = sum(x["ammonia"] for x in items) / count
            avg_turb = sum(x["turbidity_ntu"] for x in items) / count

            # Simplified CCME formula
            fails = 0
            if avg_ph < 6.5 or avg_ph > 8.5:
                fails += 1
            if avg_do < 5.0:
                fails += 1
            if avg_amm > 0.3:
                fails += 1
            if avg_turb > 50.0:
                fails += 1

            f1 = (fails / 4.0) * 100.0
            f2 = (fails / 4.0) * 100.0
            wqi = 100.0 - (math.sqrt(f1**2 + f2**2) / 1.732)
            wqi = round(max(0.0, min(wqi, 100.0)), 1)

            gold_records.append(
                {
                    "station_id": st,
                    "sample_count": count,
                    "avg_temperature": round(avg_temp, 2),
                    "avg_ph": round(avg_ph, 2),
                    "avg_do": round(avg_do, 2),
                    "avg_ammonia": round(avg_amm, 3),
                    "wqi_score": wqi,
                    "quality_label": "Good" if wqi >= 80.0 else "Fair" if wqi >= 65.0 else "Poor",
                }
            )
        return gold_records

    @staticmethod
    def generate_ml_features(series: list) -> list:
        """Computes lags, rolling stats, and cyclical time encodings for an ordered time series."""
        features = []
        for i, item in enumerate(series):
            hour = datetime.fromisoformat(item["event_time"]).hour
            pi = math.pi
            h_sin = math.sin(hour * 2.0 * pi / 24.0)
            h_cos = math.cos(hour * 2.0 * pi / 24.0)

            do_lag_1 = series[i - 1]["dissolved_oxygen"] if i >= 1 else None
            do_lag_2 = series[i - 2]["dissolved_oxygen"] if i >= 2 else None
            delta_do = (item["dissolved_oxygen"] - do_lag_1) if do_lag_1 is not None else None

            # Rolling 3 mean
            window_slice = series[max(0, i - 2) : i + 1]
            rolling_mean = sum(x["dissolved_oxygen"] for x in window_slice) / len(window_slice)

            # Target next 1
            target_next = series[i + 1]["dissolved_oxygen"] if i + 1 < len(series) else None

            features.append(
                {
                    "station_id": item["station_id"],
                    "event_time": item["event_time"],
                    "dissolved_oxygen": item["dissolved_oxygen"],
                    "do_lag_1": do_lag_1,
                    "do_lag_2": do_lag_2,
                    "delta_do": delta_do,
                    "do_rolling_mean_3": round(rolling_mean, 2),
                    "hour_sin": round(h_sin, 4),
                    "hour_cos": round(h_cos, 4),
                    "target_do_next_1": target_next,
                }
            )
        return features


class TestMedallionTransformations(unittest.TestCase):
    """Test suite for Medallion layer logic."""

    def setUp(self):
        self.sim = MedallionLogicSimulator()

    def test_bronze_layer_wraps_raw_payload(self):
        raw = json.dumps({"station_id": "ST_01", "temperature": 25.0})
        row = self.sim.bronze_transform("ST_01", raw, offset=55)
        self.assertEqual(row["kafka_key"], "ST_01")
        self.assertEqual(row["raw_payload"], raw)
        self.assertEqual(row["kafka_offset"], 55)
        self.assertRegex(row["ingest_date"], r"^\d{4}-\d{2}-\d{2}$")

    def test_silver_cleans_and_extracts_partitions(self):
        raw = json.dumps(
            {
                "station_id": "ST_HONG_01",
                "timestamp": "2026-11-04T14:30:00Z",
                "temperature": 26.5,
                "ph": 7.4,
                "dissolved_oxygen": 7.8,
                "ammonia": 0.12,
            }
        )
        bronze_row = self.sim.bronze_transform("ST_HONG_01", raw)
        is_valid, silver_rec = self.sim.silver_transform_record(bronze_row)

        self.assertTrue(is_valid)
        self.assertEqual(silver_rec["station_id"], "ST_HONG_01")
        self.assertEqual(silver_rec["year"], 2026)
        self.assertEqual(silver_rec["month"], 11)
        self.assertEqual(silver_rec["ph"], 7.4)

    def test_silver_rejects_corrupted_ph_bounds(self):
        raw = json.dumps(
            {
                "station_id": "ST_HONG_01",
                "timestamp": "2026-11-04T14:30:00Z",
                "ph": 18.5,  # Invalid pH (> 14)
                "dissolved_oxygen": 7.0,
            }
        )
        bronze_row = self.sim.bronze_transform("ST_HONG_01", raw)
        is_valid, _ = self.sim.silver_transform_record(bronze_row)
        self.assertFalse(is_valid)

    def test_gold_computes_wqi_and_aggregates(self):
        records = [
            {
                "station_id": "ST_DA_01",
                "temperature": 22.0,
                "ph": 7.3,
                "dissolved_oxygen": 8.5,
                "ammonia": 0.05,
                "turbidity_ntu": 15.0,
            },
            {
                "station_id": "ST_DA_01",
                "temperature": 22.4,
                "ph": 7.4,
                "dissolved_oxygen": 8.3,
                "ammonia": 0.06,
                "turbidity_ntu": 16.0,
            },
            {
                "station_id": "ST_SAIGON_02",
                "temperature": 28.0,
                "ph": 6.8,
                "dissolved_oxygen": 4.2,
                "ammonia": 0.85,
                "turbidity_ntu": 75.0,
            },
        ]
        gold = self.sim.compute_gold_hourly(records)
        self.assertEqual(len(gold), 2)

        # Clean river (ST_DA_01) should have high WQI
        da = next(x for x in gold if x["station_id"] == "ST_DA_01")
        self.assertEqual(da["sample_count"], 2)
        self.assertEqual(da["wqi_score"], 100.0)
        self.assertEqual(da["quality_label"], "Good")

        # More polluted river (ST_SAIGON_02) should have lower WQI
        sg = next(x for x in gold if x["station_id"] == "ST_SAIGON_02")
        self.assertLess(sg["wqi_score"], 80.0)

    def test_ml_feature_store_generates_lags_and_cyclical(self):
        time_series = [
            {"station_id": "ST_01", "event_time": "2026-11-04T00:00:00Z", "dissolved_oxygen": 8.0},
            {"station_id": "ST_01", "event_time": "2026-11-04T01:00:00Z", "dissolved_oxygen": 7.8},
            {"station_id": "ST_01", "event_time": "2026-11-04T02:00:00Z", "dissolved_oxygen": 7.5},
            {"station_id": "ST_01", "event_time": "2026-11-04T03:00:00Z", "dissolved_oxygen": 7.2},
        ]
        feat = self.sim.generate_ml_features(time_series)
        self.assertEqual(len(feat), 4)

        # Index 0 has no lags
        self.assertIsNone(feat[0]["do_lag_1"])

        # Index 1 has lag 1 = 8.0, delta = 7.8 - 8.0 = -0.2
        self.assertEqual(feat[1]["do_lag_1"], 8.0)
        self.assertAlmostEqual(feat[1]["delta_do"], -0.2, places=2)

        # Index 2 has lag 1 = 7.8, lag 2 = 8.0
        self.assertEqual(feat[2]["do_lag_1"], 7.8)
        self.assertEqual(feat[2]["do_lag_2"], 8.0)

        # Lead target for index 0 is index 1 value (7.8)
        self.assertEqual(feat[0]["target_do_next_1"], 7.8)

        # Hour 0 cyclical sin/cos
        self.assertAlmostEqual(feat[0]["hour_sin"], 0.0, places=3)
        self.assertAlmostEqual(feat[0]["hour_cos"], 1.0, places=3)


if __name__ == "__main__":
    unittest.main()
