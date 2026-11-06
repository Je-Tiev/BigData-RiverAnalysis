"""Realistic Hydrological IoT River Station Simulator.

Simulates physical water quality dynamics (temperature-DO solubility curve,
photosynthesis diurnal cycles, baseline river chemistry) across Vietnamese
river basins, with automated anomaly injection scenarios to test alerting
and Dead Letter Queue (DLQ) capabilities.
"""

import argparse
import json
import logging
import math
import os
import random
import sys
import time
from datetime import UTC, datetime
from typing import Any, ClassVar

try:
    from kafka.errors import KafkaError

    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None  # type: ignore[assignment,misc]
    KafkaError = Exception  # type: ignore[assignment,misc]

# Support running directly or as a package module
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.contracts.river_schema import (
    DEFAULT_TOPIC_SENSORS_RAW,
    REFERENCE_STATIONS,
    RiverSensorReading,
    validate_sensor_payload,
)

# Configure Structured Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s]: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("RiverSimulator")


class HydrologicalPhysicsEngine:
    """Calculates realistic sensor readings based on physical and environmental laws."""

    @staticmethod
    def calculate_do_saturation(temp_celsius: float) -> float:
        """Calculate dissolved oxygen saturation concentration (mg/L) at 1 atm pressure.

        Based on the empirical Benson-Krause freshwater solubility equation.
        """
        t = max(0.0, min(temp_celsius, 45.0))
        # Approximation formula for 100% saturation at sea level
        # At 0°C ~ 14.6 mg/L, 20°C ~ 9.1 mg/L, 30°C ~ 7.5 mg/L
        do_sat = 14.652 - 0.41022 * t + 0.007991 * (t**2) - 0.000077774 * (t**3)
        return max(5.0, round(do_sat, 2))

    @staticmethod
    def calculate_diurnal_temperature(base_temp: float, hour: int) -> float:
        """Simulate day-night solar thermal cycle (coldest at 05:00, hottest at 14:00)."""
        # Solar peak at 14:00, trough at 05:00
        phase = (hour - 5) / 24.0 * 2.0 * math.pi
        diurnal_delta = 3.5 * math.sin(phase - math.pi / 2)
        noise = random.gauss(0, 0.3)
        return round(base_temp + diurnal_delta + noise, 1)

    @staticmethod
    def compute_ccme_wqi(readings: dict[str, float]) -> tuple[float, str]:
        """Compute Canadian Council of Ministers of the Environment (CCME) Water Quality Index.

        Evaluates objectives based on QCVN 08-MT:2015/BTNMT (Surface Water Regulation).
        Returns:
            Tuple of (wqi_score: float, quality_label: str)
        """
        objectives = {
            "ph": (6.5, 8.5),
            "dissolved_oxygen": (5.0, 15.0),
            "ammonia": (0.0, 0.3),
            "bod5": (0.0, 4.0),
            "conductivity": (0.0, 500.0),
            "turbidity_ntu": (0.0, 50.0),
        }

        failed_variables = 0
        total_tests = 0
        failed_tests = 0
        sum_excursions = 0.0

        for var, (min_val, max_val) in objectives.items():
            val = readings.get(var)
            if val is None:
                continue

            total_tests += 1
            is_failed = False
            excursion = 0.0

            if val < min_val:
                is_failed = True
                excursion = (min_val / max(val, 0.001)) - 1.0
            elif val > max_val:
                is_failed = True
                excursion = (val / max_val) - 1.0

            if is_failed:
                failed_tests += 1
                failed_variables += 1
                sum_excursions += excursion

        if total_tests == 0:
            return 80.0, "Good"

        # F1: Scope (% variables failing)
        f1 = (failed_variables / len(objectives)) * 100.0

        # F2: Frequency (% tests failing)
        f2 = (failed_tests / total_tests) * 100.0

        # F3: Amplitude (Normalized Sum of Excursions)
        nse = sum_excursions / total_tests
        f3 = nse / (0.01 * nse + 0.01)

        ccme = 100.0 - (math.sqrt(f1**2 + f2**2 + min(f3, 100.0) ** 2) / 1.732)
        score = round(max(0.0, min(ccme, 100.0)), 1)

        if score >= 95.0:
            label = "Excellent"
        elif score >= 80.0:
            label = "Good"
        elif score >= 65.0:
            label = "Fair"
        elif score >= 45.0:
            label = "Marginal"
        else:
            label = "Poor"

        return score, label


class RiverTelemetryGenerator:
    """Generates continuous realistic telemetry streams with anomaly injection."""

    # Specific baseline parameters per river
    RIVER_BASELINES: ClassVar[dict[str, dict[str, Any]]] = {
        "Sông Đà": {
            "base_temp": 21.0,
            "ph": 7.4,
            "conductivity": 220.0,
            "ammonia": 0.04,
            "bod5": 1.2,
            "turbidity": 15.0,
            "flow_rate": 2200.0,
        },
        "Sông Lô": {
            "base_temp": 23.0,
            "ph": 7.3,
            "conductivity": 280.0,
            "ammonia": 0.08,
            "bod5": 1.8,
            "turbidity": 35.0,
            "flow_rate": 1100.0,
        },
        "Sông Hồng": {
            "base_temp": 24.5,
            "ph": 7.2,
            "conductivity": 380.0,
            "ammonia": 0.15,
            "bod5": 2.5,
            "turbidity": 80.0,
            "flow_rate": 3500.0,
        },
        "Sông Hương": {
            "base_temp": 25.0,
            "ph": 7.1,
            "conductivity": 180.0,
            "ammonia": 0.06,
            "bod5": 1.5,
            "turbidity": 20.0,
            "flow_rate": 450.0,
        },
        "Sông Hàn": {
            "base_temp": 26.0,
            "ph": 7.5,
            "conductivity": 420.0,
            "ammonia": 0.12,
            "bod5": 2.2,
            "turbidity": 25.0,
            "flow_rate": 380.0,
        },
        "Sông Sài Gòn": {
            "base_temp": 28.0,
            "ph": 6.8,
            "conductivity": 550.0,
            "ammonia": 0.35,
            "bod5": 4.5,
            "turbidity": 65.0,
            "flow_rate": 750.0,
        },
        "Sông Đồng Nai": {
            "base_temp": 27.5,
            "ph": 7.0,
            "conductivity": 480.0,
            "ammonia": 0.22,
            "bod5": 3.2,
            "turbidity": 45.0,
            "flow_rate": 1800.0,
        },
        "Sông Tiền": {
            "base_temp": 28.5,
            "ph": 7.3,
            "conductivity": 520.0,
            "ammonia": 0.18,
            "bod5": 2.8,
            "turbidity": 90.0,
            "flow_rate": 6500.0,
        },
        "Sông Hậu": {
            "base_temp": 28.5,
            "ph": 7.2,
            "conductivity": 510.0,
            "ammonia": 0.16,
            "bod5": 2.6,
            "turbidity": 85.0,
            "flow_rate": 5800.0,
        },
        "Sông Mã": {
            "base_temp": 23.5,
            "ph": 7.3,
            "conductivity": 310.0,
            "ammonia": 0.09,
            "bod5": 1.9,
            "turbidity": 40.0,
            "flow_rate": 850.0,
        },
    }

    def __init__(
        self,
        stations: list[dict[str, Any]] | None = None,
        anomaly_probability: float = 0.08,
    ):
        self.stations = stations or REFERENCE_STATIONS
        self.anomaly_prob = anomaly_probability
        self.physics = HydrologicalPhysicsEngine()

    def generate_single_reading(
        self,
        station: dict[str, Any],
        override_timestamp: datetime | None = None,
        force_anomaly: str | None = None,
    ) -> RiverSensorReading:
        """Generate one physically grounded reading for the given station."""
        now = override_timestamp or datetime.now(UTC)
        hour = now.hour
        iso_timestamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        river_name = station["river_name"]
        baseline = self.RIVER_BASELINES.get(river_name, self.RIVER_BASELINES["Sông Hồng"])

        # 1. Physics-based baseline computation
        temp = self.physics.calculate_diurnal_temperature(baseline["base_temp"], hour)
        do_sat = self.physics.calculate_do_saturation(temp)

        # Natural saturation ratio (85% - 98% with daylight photosynthetic boost)
        daylight_boost = 0.08 * math.sin(math.pi * max(0, min(hour - 6, 12)) / 12.0)
        saturation_ratio = random.uniform(0.85, 0.95) + daylight_boost
        dissolved_oxygen = round(do_sat * saturation_ratio, 2)

        # Baseline chemical properties with natural stochastic micro-variation
        ph = round(baseline["ph"] + random.gauss(0, 0.08), 2)
        conductivity = round(baseline["conductivity"] + random.gauss(0, 8.0), 1)
        ammonia = round(max(0.01, baseline["ammonia"] + random.gauss(0, 0.015)), 3)
        bod5 = round(max(0.5, baseline["bod5"] + random.gauss(0, 0.15)), 1)
        chloride = round(max(5.0, conductivity * 0.08 + random.gauss(0, 1.5)), 1)
        total_hardness = round(max(30.0, conductivity * 0.45 + random.gauss(0, 5.0)), 1)
        turbidity_ntu = round(max(2.0, baseline["turbidity"] + random.gauss(0, 3.5)), 1)
        flow_rate_m3s = round(max(10.0, baseline["flow_rate"] + random.gauss(0, 40.0)), 1)

        # 2. Anomaly Injection Scenarios
        is_anomaly = False
        anomaly_reason = None
        trigger_anomaly = force_anomaly or (random.random() < self.anomaly_prob)

        if trigger_anomaly:
            scenario = force_anomaly or random.choice(
                [
                    "INDUSTRIAL_ACID_DISCHARGE",
                    "UNTREATED_SEWAGE_DUMP",
                    "FLASH_RUNOFF_EROSION",
                    "SUDDEN_ANOXIA",
                ]
            )
            is_anomaly = True

            if scenario == "INDUSTRIAL_ACID_DISCHARGE":
                ph = round(random.uniform(3.8, 5.2), 2)
                conductivity = round(conductivity * random.uniform(2.5, 4.0), 1)
                ammonia = round(ammonia * random.uniform(2.0, 5.0), 3)
                dissolved_oxygen = round(max(1.2, dissolved_oxygen * 0.45), 2)
                anomaly_reason = "Sự cố xả thải công nghiệp: pH giảm mạnh, độ dẫn điện và ammonia tăng vọt"

            elif scenario == "UNTREATED_SEWAGE_DUMP":
                ammonia = round(random.uniform(2.5, 7.8), 3)
                bod5 = round(random.uniform(25.0, 55.0), 1)
                dissolved_oxygen = round(max(0.8, dissolved_oxygen * 0.3), 2)
                turbidity_ntu = round(turbidity_ntu * random.uniform(2.5, 4.5), 1)
                anomaly_reason = "Xả thải sinh hoạt/hữu cơ nghiêm trọng: Ammonia và BOD5 vượt ngưỡng cực đại"

            elif scenario == "FLASH_RUNOFF_EROSION":
                turbidity_ntu = round(random.uniform(250.0, 680.0), 1)
                flow_rate_m3s = round(flow_rate_m3s * random.uniform(2.0, 3.5), 1)
                anomaly_reason = "Lũ quét / Sạt lở ven sông: Độ đục và lưu lượng dòng chảy tăng đột biến"

            elif scenario == "SUDDEN_ANOXIA":
                dissolved_oxygen = round(random.uniform(0.5, 2.2), 2)
                anomaly_reason = "Hiện tượng cạn kiệt oxy (Anoxia): DO giảm dưới ngưỡng sinh tồn của thủy sinh"

        # 3. Calculate CCME WQI
        metric_dict = {
            "ph": ph,
            "dissolved_oxygen": dissolved_oxygen,
            "ammonia": ammonia,
            "bod5": bod5,
            "conductivity": conductivity,
            "turbidity_ntu": turbidity_ntu,
        }
        ccme_score, quality_label = self.physics.compute_ccme_wqi(metric_dict)

        return RiverSensorReading(
            station_id=station["station_id"],
            station_name=station["station_name"],
            river_name=station["river_name"],
            basin_name=station["basin_name"],
            latitude=station["latitude"],
            longitude=station["longitude"],
            timestamp=iso_timestamp,
            temperature=temp,
            ph=ph,
            dissolved_oxygen=dissolved_oxygen,
            conductivity=conductivity,
            ammonia=ammonia,
            bod5=bod5,
            chloride=chloride,
            total_hardness=total_hardness,
            turbidity_ntu=turbidity_ntu,
            flow_rate_m3s=flow_rate_m3s,
            ccme_score=ccme_score,
            quality_label=quality_label,
            is_anomaly=is_anomaly,
            anomaly_reason=anomaly_reason,
        )

    def generate_batch(self, count: int = 10) -> list[dict[str, Any]]:
        """Generate a batch of validated sensor records."""
        records = []
        for i in range(count):
            station = self.stations[i % len(self.stations)]
            reading = self.generate_single_reading(station)
            records.append(reading.to_dict())
        return records


def run_simulator(
    kafka_server: str = "localhost:29092",
    topic: str = DEFAULT_TOPIC_SENSORS_RAW,
    delay: float = 1.0,
    dry_run: bool = False,
    max_records: int | None = None,
):
    """Main execution loop for the river simulator."""
    generator = RiverTelemetryGenerator(anomaly_probability=0.08)
    producer = None

    if not dry_run:
        try:
            if KafkaProducer is None:
                raise ImportError("kafka-python package not available")

            producer = KafkaProducer(
                bootstrap_servers=kafka_server,
                value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                acks=1,
                retries=3,
            )
            logger.info("Connected to Kafka Producer at %s (Topic: %s)", kafka_server, topic)
        except (KafkaError, OSError, ConnectionError, ImportError) as e:
            logger.error("Failed to connect to Kafka at %s: %s", kafka_server, e)
            logger.info("Switching to DRY-RUN mode for console output...")
            dry_run = True

    logger.info("Starting River Water Quality Simulator (Delay: %.1fs, Dry-Run: %s)", delay, dry_run)
    record_count = 0

    try:
        while True:
            for station in REFERENCE_STATIONS:
                reading = generator.generate_single_reading(station)
                payload = reading.to_dict()

                # Contract validation check before sending
                is_valid, error = validate_sensor_payload(payload)
                if not is_valid:
                    logger.warning("Generated payload failed validation: %s", error)
                    continue

                if producer and not dry_run:
                    producer.send(topic, key=reading.station_id.encode("utf-8"), value=payload)

                alert_flag = "🚨 [ANOMALY]" if reading.is_anomaly else "🟢 [NORMAL]"
                logger.info(
                    "%s %s | %s | Temp: %.1f°C | pH: %.2f | DO: %.2f mg/L | WQI: %.1f (%s)",
                    alert_flag,
                    reading.station_id,
                    reading.river_name,
                    reading.temperature,
                    reading.ph,
                    reading.dissolved_oxygen,
                    reading.ccme_score or 0.0,
                    reading.quality_label,
                )
                if reading.is_anomaly:
                    logger.warning("   └── Lý do: %s", reading.anomaly_reason)

                record_count += 1
                if max_records and record_count >= max_records:
                    logger.info("Reached maximum record limit (%d). Terminating.", max_records)
                    return

                time.sleep(delay)

    except KeyboardInterrupt:
        logger.info("Simulator halted by operator.")
    finally:
        if producer:
            producer.flush()
            producer.close()
            logger.info("Kafka Producer connection closed cleanly.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Realistic Hydrological River Telemetry Simulator")
    parser.add_argument("--kafka", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"), help="Kafka server")
    parser.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_RAW", DEFAULT_TOPIC_SENSORS_RAW), help="Kafka topic")
    parser.add_argument("--delay", type=float, default=1.0, help="Delay between station readings in seconds")
    parser.add_argument("--dry-run", action="store_true", help="Print readings without publishing to Kafka")
    parser.add_argument("--count", type=int, default=None, help="Stop after generating N records")

    args = parser.parse_args()
    run_simulator(
        kafka_server=args.kafka,
        topic=args.topic,
        delay=args.delay,
        dry_run=args.dry_run,
        max_records=args.count,
    )
