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

    def test_schema_v1_3_expanded_fields_serialization_and_from_dict(self):
        payload = {
            "schema_version": "1.3.0",
            "station_id": "ST_TEST_02",
            "station_name": "Trạm Thử Nghiệm v1.3",
            "river_name": "Sông Hồng",
            "basin_name": "Lưu vực sông Hồng - Thái Bình",
            "latitude": 21.0,
            "longitude": 105.8,
            "timestamp": "2026-11-13T12:00:00Z",
            "temperature": 26.5,
            "ph": 7.4,
            "dissolved_oxygen": 7.8,
            "conductivity": 350.0,
            "ammonia": 0.08,
            "bod5": 1.9,
            "chloride": 22.0,
            "total_hardness": 130.0,
            "turbidity_ntu": 25.0,
            "flow_rate_m3s": 2500.0,
            "cod": 6.2,
            "tss": 38.0,
            "nitrate_no3": 1.1,
            "phosphate_po4": 0.04,
            "coliform": 1200.0,
            "salinity": 0.04,
            "alkalinity": 135.0,
            "true_colour": 15.0,
            "ccme_score": 91.0,
            "quality_label": "Excellent",
            "vn_wqi_score": 88.5,
            "vn_wqi_label": "Tốt",
            "qcvn_tier": "A",
            "is_anomaly": False,
        }
        reading = RiverSensorReading.from_dict(payload)
        self.assertEqual(reading.cod, 6.2)
        self.assertEqual(reading.tss, 38.0)
        self.assertEqual(reading.nitrate_no3, 1.1)
        self.assertEqual(reading.phosphate_po4, 0.04)
        self.assertEqual(reading.coliform, 1200.0)
        self.assertEqual(reading.salinity, 0.04)
        self.assertEqual(reading.alkalinity, 135.0)
        self.assertEqual(reading.true_colour, 15.0)
        self.assertEqual(reading.vn_wqi_score, 88.5)
        self.assertEqual(reading.vn_wqi_label, "Tốt")
        self.assertEqual(reading.qcvn_tier, "A")

        data_dict = reading.to_dict()
        self.assertEqual(data_dict["schema_version"], "1.3.0")
        self.assertEqual(data_dict["cod"], 6.2)
        self.assertEqual(data_dict["vn_wqi_label"], "Tốt")


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

    def test_vn_wqi_pristine_water(self):
        pristine = {
            "ph": 7.5,
            "dissolved_oxygen": 7.8,
            "bod5": 2.0,
            "cod": 8.0,
            "ammonia": 0.08,
            "nitrate_no3": 0.8,
            "phosphate_po4": 0.03,
            "coliform": 800.0,
        }
        score, label = self.physics.compute_vn_wqi(pristine)
        self.assertGreaterEqual(score, 90.0)
        self.assertEqual(label, "Rất tốt")

    def test_vn_wqi_heavily_polluted_water(self):
        polluted = {
            "ph": 4.5,
            "dissolved_oxygen": 1.2,
            "bod5": 35.0,
            "cod": 95.0,
            "ammonia": 3.5,
            "nitrate_no3": 18.0,
            "phosphate_po4": 1.2,
            "coliform": 250000.0,
        }
        score, label = self.physics.compute_vn_wqi(polluted)
        self.assertLess(score, 30.0)
        self.assertIn(label, ["Kém", "Ô nhiễm nặng"])


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
        reading = self.generator.generate_single_reading(station=station, force_anomaly="INDUSTRIAL_ACID_DISCHARGE")
        self.assertTrue(reading.is_anomaly)
        self.assertIsNotNone(reading.anomaly_reason)
        self.assertLess(reading.ph, 6.0)
        self.assertGreater(reading.conductivity, 500.0)

    def test_anomaly_injection_sewage_dump(self):
        station = REFERENCE_STATIONS[1]
        reading = self.generator.generate_single_reading(station=station, force_anomaly="UNTREATED_SEWAGE_DUMP")
        self.assertTrue(reading.is_anomaly)
        self.assertGreater(reading.ammonia, 2.0)
        self.assertGreater(reading.bod5, 20.0)

    def test_anomaly_injection_tidal_salinity(self):
        station = REFERENCE_STATIONS[5]  # Sông Sài Gòn
        reading = self.generator.generate_single_reading(station=station, force_anomaly="TIDAL_SALINITY_INTRUSION")
        self.assertTrue(reading.is_anomaly)
        self.assertIsNotNone(reading.anomaly_reason)
        self.assertIn("Xâm nhập mặn", reading.anomaly_reason)
        self.assertGreater(reading.salinity, 5.0)

    def test_anomaly_injection_eutrophication(self):
        station = REFERENCE_STATIONS[0]
        reading = self.generator.generate_single_reading(station=station, force_anomaly="EUTROPHICATION_ALGAL_BLOOM")
        self.assertTrue(reading.is_anomaly)
        self.assertIsNotNone(reading.anomaly_reason)
        self.assertIn("phú dưỡng hóa", reading.anomaly_reason)
        self.assertGreater(reading.phosphate_po4, 0.3)
        self.assertGreater(reading.nitrate_no3, 5.0)

    def test_generator_populates_all_18_parameters_and_dual_wqi(self):
        station = REFERENCE_STATIONS[0]
        reading = self.generator.generate_single_reading(station=station)
        # Verify all 18 physicochemical and hydrological parameters are non-null
        fields_to_check = [
            "temperature",
            "ph",
            "dissolved_oxygen",
            "conductivity",
            "ammonia",
            "bod5",
            "chloride",
            "total_hardness",
            "turbidity_ntu",
            "flow_rate_m3s",
            "cod",
            "tss",
            "nitrate_no3",
            "phosphate_po4",
            "coliform",
            "salinity",
            "alkalinity",
            "true_colour",
        ]
        data = reading.to_dict()
        for f in fields_to_check:
            self.assertIsNotNone(data[f], f"Field '{f}' should not be None")
        self.assertIsNotNone(reading.ccme_score)
        self.assertIsNotNone(reading.vn_wqi_score)
        self.assertIsNotNone(reading.vn_wqi_label)


if __name__ == "__main__":
    unittest.main()
