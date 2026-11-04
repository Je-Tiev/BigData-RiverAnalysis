"""Unit Tests for Data Quality Auditor and Integrity Gates.

Validates assertions on completeness, null rates, physical boundary compliance,
and JSON audit report generation.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.quality.data_auditor import RiverDataQualityAuditor


class TestDataQualityAuditor(unittest.TestCase):
    """Test suite for RiverDataQualityAuditor."""

    def setUp(self):
        self.auditor = RiverDataQualityAuditor(
            max_null_rate_tolerance=0.0,
            max_physical_violation_rate=0.01,
        )
        self.clean_records = [
            {
                "station_id": f"ST_CLEAN_{i}",
                "timestamp": "2026-11-04T12:00:00Z",
                "temperature": 24.0,
                "ph": 7.3,
                "dissolved_oxygen": 7.5,
                "ammonia": 0.1,
                "conductivity": 350.0,
                "turbidity_ntu": 15.0,
            }
            for i in range(10)
        ]

    def test_clean_dataset_passes_audit(self):
        report = self.auditor.audit_records(self.clean_records)
        self.assertEqual(report.overall_status, "PASSED")
        self.assertEqual(report.failed_checks, 0)
        self.assertEqual(report.total_records, 10)

    def test_missing_station_id_fails_audit(self):
        corrupt_records = [dict(r) for r in self.clean_records]
        corrupt_records[0]["station_id"] = None

        report = self.auditor.audit_records(corrupt_records)
        self.assertEqual(report.overall_status, "FAILED")
        self.assertGreater(report.failed_checks, 0)

        null_check = next(c for c in report.results if c.check_name == "null_station_id_check")
        self.assertFalse(null_check.passed)
        self.assertEqual(null_check.violation_count, 1)

    def test_missing_timestamp_fails_audit(self):
        corrupt_records = [dict(r) for r in self.clean_records]
        corrupt_records[1]["timestamp"] = ""

        report = self.auditor.audit_records(corrupt_records)
        self.assertEqual(report.overall_status, "FAILED")

    def test_physical_boundary_violation_detected(self):
        corrupt_records = [dict(r) for r in self.clean_records]
        corrupt_records[2]["ph"] = -1.5  # Impossible pH

        report = self.auditor.audit_records(corrupt_records)
        ph_check = next(c for c in report.results if c.check_name == "ph_range_check")
        self.assertFalse(ph_check.passed)
        self.assertEqual(ph_check.violation_count, 1)

    def test_empty_dataset_returns_warning(self):
        report = self.auditor.audit_records([])
        self.assertEqual(report.overall_status, "WARNING")
        self.assertEqual(report.total_records, 0)

    def test_report_json_serialization(self):
        report = self.auditor.audit_records(self.clean_records)
        report_json = report.to_json()
        parsed = json.loads(report_json)
        self.assertEqual(parsed["suite_name"], "RiverLakehouse_Quality_Suite")
        self.assertEqual(parsed["overall_status"], "PASSED")
        self.assertIsInstance(parsed["results"], list)


if __name__ == "__main__":
    unittest.main()
