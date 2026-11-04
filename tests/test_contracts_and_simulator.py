"""Unit Tests for River Quality Data Contracts and IoT Simulator.

Tests data validation, physical boundary checks, diurnal physics, CCME WQI
computation, and anomaly injection scenarios.
"""

import os
import sys
import unittest

# Add root directory to python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.contracts.river_schema import (
    REFERENCE_STATIONS,
    RiverSensorReading,
    validate_sensor_payload,
)
from src.producers.river_simulator import (
    HydrologicalPhysicsEngine,
    RiverTelemetryGenerator,
)


class TestRiverDataContracts(unittest.TestCase):
    """Test suite for Data Contract schemas and validation logic."""

    def setUp(self):
        self.valid_payload = {
            "schema_version": "1.2.0",
            "station_id": "ST_HONG_LB_01",
            "station_name": "Trạm Cầu Long Biên",
            "river_name": "Sông Hồng",
            "basin_name": "Lưu vực sông Hồng - Thái Bình",
            "latitude": 21.0425,
            "longitude": 105.8614,
            "timestamp": "2026-11-04T20:30:00Z",
            "temperature": 24.5,
            "ph": 7.35,
            "dissolved_oxygen": 7.12,
            "conductivity": 380.0,
            "ammonia": 0.12,
            "bod5": 2.4,
            "chloride": 22.5,
            "total_hardness": 145.0,
            "turbidity_ntu": 45.0,
            "flow_rate_m3s": 3200.0,
        }

    def test_valid_payload_passes_validation(self):
        is_valid, error = validate_sensor_payload(self.valid_payload)
        self.assertTrue(is_valid)
        self.assertIsNone(error)

    def test_missing_required_field_fails(self):
        invalid_payload = self.valid_payload.copy()
        del invalid_payload["dissolved_oxygen"]
        is_valid, error = validate_sensor_payload(invalid_payload)
        self.assertFalse(is_valid)
        self.assertIn("Missing required field", error)

    def test_invalid_timestamp_fails(self):
        invalid_payload = self.valid_payload.copy()
        invalid_payload["timestamp"] = "invalid-date-string"
        is_valid, error = validate_sensor_payload(invalid_payload)
        self.assertFalse(is_valid)
        self.assertIn("Invalid timestamp format", error)

    def test_out_of_bounds_ph_fails(self):
        invalid_payload = self.valid_payload.copy()
        invalid_payload["ph"] = 14.8  # pH scale cannot exceed 14.0
        is_valid, error = validate_sensor_payload(invalid_payload)
        self.assertFalse(is_valid)
        self.assertIn("outside physical bounds", error)

    def test_out_of_bounds_negative_ammonia_fails(self):
        invalid_payload = self.valid_payload.copy()
        invalid_payload["ammonia"] = -0.5
        is_valid, error = validate_sensor_payload(invalid_payload)
        self.assertFalse(is_valid)
        self.assertIn("outside physical bounds", error)

    def test_reading_dataclass_serialization(self):
        reading = RiverSensorReading(
            station_id="ST_TEST_01",
            station_name="Trạm Test",
            river_name="Sông Test",
            basin_name="Lưu vực Test",
            latitude=21.0,
            longitude=105.0,
            timestamp="2026-11-04T12:00:00Z",
            temperature=25.0,
            ph=7.0,
            dissolved_oxygen=8.0,
            conductivity=300.0,
            ammonia=0.1,
            bod5=2.0,
            chloride=20.0,
            total_hardness=100.0,
            turbidity_ntu=10.0,
            flow_rate_m3s=500.0,
            ccme_score=92.5,
            quality_label="Good",
        )
        data = reading.to_dict()
        self.assertEqual(data["station_id"], "ST_TEST_01")
        self.assertEqual(data["quality_label"], "Good")
        self.assertEqual(data["temperature"], 25.0)


class TestHydrologicalPhysicsEngine(unittest.TestCase):
    """Test suite for physics calculations (temperature, DO solubility, CCME WQI)."""

    def setUp(self):
        self.physics = HydrologicalPhysicsEngine()

    def test_do_saturation_decreases_with_temperature(self):
        cold_do = self.physics.calculate_do_saturation(10.0)
        warm_do = self.physics.calculate_do_saturation(30.0)
        self.assertGreater(cold_do, warm_do)
        self.assertGreaterEqual(warm_do, 5.0)

    def test_diurnal_temperature_cycle(self):
        # 14:00 should typically be warmer than 05:00
        temp_night = self.physics.calculate_diurnal_temperature(25.0, hour=5)
        temp_day = self.physics.calculate_diurnal_temperature(25.0, hour=14)
        self.assertGreater(temp_day, temp_night)

    def test_ccme_wqi_pristine_water(self):
        pristine = {
            "ph": 7.4,
            "dissolved_oxygen": 8.5,
            "ammonia": 0.03,
            "bod5": 1.2,
            "conductivity": 200.0,
            "turbidity_ntu": 12.0,
        }
        score, label = self.physics.compute_ccme_wqi(pristine)
        self.assertGreaterEqual(score, 90.0)
        self.assertIn(label, ["Excellent", "Good"])

    def test_ccme_wqi_heavily_polluted_water(self):
        polluted = {
            "ph": 4.1,
            "dissolved_oxygen": 1.5,
            "ammonia": 5.2,
            "bod5": 45.0,
            "conductivity": 1800.0,
            "turbidity_ntu": 420.0,
        }
        score, label = self.physics.compute_ccme_wqi(polluted)
        self.assertLess(score, 50.0)
        self.assertIn(label, ["Marginal", "Poor"])


class TestRiverTelemetryGenerator(unittest.TestCase):
    """Test suite for the IoT Generator and anomaly injection."""

    def setUp(self):
        self.generator = RiverTelemetryGenerator(anomaly_probability=0.0)

    def test_generator_produces_all_reference_stations(self):
        self.assertEqual(len(REFERENCE_STATIONS), 10)
        batch = self.generator.generate_batch(count=10)
        self.assertEqual(len(batch), 10)
        for record in batch:
            is_valid, error = validate_sensor_payload(record)
            self.assertTrue(is_valid, f"Generated record failed validation: {error}")
            self.assertIsNotNone(record["ccme_score"])
            self.assertIn(record["quality_label"], ["Excellent", "Good", "Fair", "Marginal", "Poor"])

    def test_anomaly_injection_acid_discharge(self):
        station = REFERENCE_STATIONS[0]
        reading = self.generator.generate_single_reading(
            station=station,
            force_anomaly="INDUSTRIAL_ACID_DISCHARGE"
        )
        self.assertTrue(reading.is_anomaly)
        self.assertIsNotNone(reading.anomaly_reason)
        self.assertLess(reading.ph, 6.0)
        self.assertGreater(reading.conductivity, 500.0)

    def test_anomaly_injection_sewage_dump(self):
        station = REFERENCE_STATIONS[1]
        reading = self.generator.generate_single_reading(
            station=station,
            force_anomaly="UNTREATED_SEWAGE_DUMP"
        )
        self.assertTrue(reading.is_anomaly)
        self.assertGreater(reading.ammonia, 2.0)
        self.assertGreater(reading.bod5, 20.0)


if __name__ == "__main__":
    unittest.main()
