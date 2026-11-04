"""Unit Tests for Streaming Transformation Logic, DLQ, and Normalization.

Tests record parsing, backward compatibility with legacy schema, DLQ routing,
and critical pollution alert evaluation without requiring live Kafka or Spark.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


class StreamingTransformSimulator:
    """Simulates the SQL transformations performed inside Spark Streaming."""

    @staticmethod
    def is_dlq_candidate(raw_string: str) -> tuple[bool, str]:
        """Simulates corrupt_stream filter logic."""
        if not raw_string or not raw_string.strip():
            return True, "EMPTY_PAYLOAD"
        try:
            parsed = json.loads(raw_string)
        except Exception as e:
            return True, f"JSON_DECODE_ERROR: {str(e)}"

        if not isinstance(parsed, dict):
            return True, "PAYLOAD_NOT_OBJECT"

        has_id = bool(parsed.get("station_id") or parsed.get("WaterbodyName"))
        if not has_id:
            return True, "MISSING_STATION_IDENTIFIER"

        return False, "VALID"

    @staticmethod
    def normalize_record(parsed: dict) -> dict:
        """Simulates normalized_stream column mappings and backward compatibility."""
        station_id = parsed.get("station_id") or parsed.get("WaterbodyName") or "UNKNOWN"
        river_name = parsed.get("river_name") or parsed.get("WaterbodyName") or "UNKNOWN"
        timestamp = parsed.get("timestamp") or parsed.get("FullDate")

        # Conductivity handling (standardizing key encoding)
        cond = parsed.get("conductivity")
        if cond is None:
            cond = parsed.get("Conductivity @25°C") or parsed.get("Conductivity @25Â°C")

        # Ammonia handling
        ammonia = parsed.get("ammonia")
        if ammonia is None:
            ammonia = parsed.get("Ammonia-Total (as N)")

        # BOD5 handling
        bod5 = parsed.get("bod5")
        if bod5 is None:
            bod5 = parsed.get("BOD - 5 days (Total)")

        # Hardness handling
        hardness = parsed.get("total_hardness")
        if hardness is None:
            hardness = parsed.get("Total Hardness (as CaCO3)")

        # CCME Score
        ccme = parsed.get("ccme_score")
        if ccme is None and "CCME_Values" in parsed:
            try:
                ccme = float(parsed["CCME_Values"])
            except (ValueError, TypeError):
                ccme = None

        # Critical Pollution Flag (QCVN 08-MT:2015/BTNMT)
        do_val = float(parsed.get("dissolved_oxygen") or 0.0)
        ph_val = float(parsed.get("ph") or 7.0)
        amm_val = float(ammonia or 0.0)

        is_critical = (do_val < 4.0) or (ph_val < 5.5) or (ph_val > 9.0) or (amm_val > 1.5)

        # Quality Label
        label = parsed.get("quality_label")
        if not label and ccme is not None:
            if ccme >= 95.0:
                label = "Excellent"
            elif ccme >= 80.0:
                label = "Good"
            elif ccme >= 65.0:
                label = "Fair"
            elif ccme >= 45.0:
                label = "Marginal"
            else:
                label = "Poor"

        return {
            "station_id": station_id,
            "river_name": river_name,
            "timestamp": timestamp,
            "conductivity": float(cond) if cond is not None else None,
            "ammonia": float(ammonia) if ammonia is not None else None,
            "bod5": float(bod5) if bod5 is not None else None,
            "total_hardness": float(hardness) if hardness is not None else None,
            "ccme_score": float(ccme) if ccme is not None else None,
            "quality_label": label,
            "is_critical_pollution": is_critical,
        }


class TestStreamingTransformations(unittest.TestCase):
    """Test suite for Streaming DLQ and Normalization."""

    def setUp(self):
        self.sim = StreamingTransformSimulator()

    def test_dlq_catches_invalid_json(self):
        is_dlq, reason = self.sim.is_dlq_candidate("INVALID_JSON_{{{")
        self.assertTrue(is_dlq)
        self.assertIn("JSON_DECODE_ERROR", reason)

    def test_dlq_catches_empty_payload(self):
        is_dlq, reason = self.sim.is_dlq_candidate("")
        self.assertTrue(is_dlq)
        self.assertEqual(reason, "EMPTY_PAYLOAD")

    def test_dlq_catches_missing_station_identifier(self):
        is_dlq, reason = self.sim.is_dlq_candidate(json.dumps({"temperature": 25.0, "ph": 7.0}))
        self.assertTrue(is_dlq)
        self.assertEqual(reason, "MISSING_STATION_IDENTIFIER")

    def test_dlq_passes_valid_canonical_payload(self):
        valid = json.dumps({"station_id": "ST_01", "temperature": 24.0, "ph": 7.2})
        is_dlq, reason = self.sim.is_dlq_candidate(valid)
        self.assertFalse(is_dlq)
        self.assertEqual(reason, "VALID")

    def test_dlq_passes_valid_legacy_payload(self):
        legacy = json.dumps({"WaterbodyName": "Sông Hồng", "FullDate": "2024-01-01"})
        is_dlq, reason = self.sim.is_dlq_candidate(legacy)
        self.assertFalse(is_dlq)
        self.assertEqual(reason, "VALID")

    def test_normalization_maps_legacy_columns_correctly(self):
        legacy_record = {
            "WaterbodyName": "Thames River",
            "FullDate": "2020-01-01",
            "Conductivity @25°C": "450.5",
            "Ammonia-Total (as N)": "0.25",
            "BOD - 5 days (Total)": "2.1",
            "Total Hardness (as CaCO3)": "120.0",
            "CCME_Values": "84.5",
            "dissolved_oxygen": 6.8,
            "ph": 7.3,
        }
        res = self.sim.normalize_record(legacy_record)
        self.assertEqual(res["station_id"], "Thames River")
        self.assertEqual(res["river_name"], "Thames River")
        self.assertEqual(res["conductivity"], 450.5)
        self.assertEqual(res["ammonia"], 0.25)
        self.assertEqual(res["bod5"], 2.1)
        self.assertEqual(res["quality_label"], "Good")
        self.assertFalse(res["is_critical_pollution"])

    def test_critical_pollution_triggers_on_low_do(self):
        record = {
            "station_id": "ST_CRITICAL",
            "dissolved_oxygen": 2.1,  # Critical (< 4.0 mg/L)
            "ph": 7.2,
            "ammonia": 0.1,
            "ccme_score": 35.0,
        }
        res = self.sim.normalize_record(record)
        self.assertTrue(res["is_critical_pollution"])
        self.assertEqual(res["quality_label"], "Poor")

    def test_critical_pollution_triggers_on_high_ammonia(self):
        record = {
            "station_id": "ST_CRITICAL_AMM",
            "dissolved_oxygen": 6.5,
            "ph": 7.2,
            "ammonia": 2.8,  # Critical (> 1.5 mg/L)
            "ccme_score": 50.0,
        }
        res = self.sim.normalize_record(record)
        self.assertTrue(res["is_critical_pollution"])
        self.assertEqual(res["quality_label"], "Marginal")


if __name__ == "__main__":
    unittest.main()
