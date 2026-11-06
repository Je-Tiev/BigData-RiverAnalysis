"""Alert Manager & Emergency Notification Dispatcher.

Handles packaging, enrichment, deduplication, and publishing of critical water
pollution alerts to Kafka topic river.alerts.critical and operational channels.
"""

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any, ClassVar

try:
    from kafka.errors import KafkaError

    from kafka import KafkaProducer
except ImportError:
    KafkaProducer = None  # type: ignore[assignment,misc]
    KafkaError = Exception  # type: ignore[assignment,misc]

logger = logging.getLogger("AlertManager")


@dataclass
class CriticalAlertMessage:
    """Standardized critical pollution alert schema."""

    alert_id: str
    station_id: str
    station_name: str
    river_name: str
    timestamp: str
    severity: str  # CRITICAL | WARNING
    alert_type: str  # ANOMALY_SURGE | TOXIC_DISCHARGE | ANOXIA_RISK
    reasons: list[str]
    metrics: dict[str, float]
    recommended_action: str
    created_at: str = field(default_factory=lambda: datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"))

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False)


class RiverAlertManager:
    """Manages the creation and dispatching of real-time river pollution alarms."""

    RECOMMENDED_ACTIONS: ClassVar[dict[str, str]] = {
        "DISSOLVED_OXYGEN": "Khẩn cấp: Tăng cường sục khí cục bộ; cảnh báo ngư dân khu vực lân cận ngừng cấp nước vào ao nuôi.",
        "ACID_SPILL": "Báo động đỏ: Khẩn cấp điều tra cống xả thải công nghiệp thượng nguồn; lấy mẫu nước niêm phong hiện trường.",
        "AMMONIA_SEWAGE": "Cảnh báo ô nhiễm hữu cơ: Kiểm tra hệ thống xử lý nước thải đô thị; hạn chế sử dụng nước mặt cho sinh hoạt.",
        "TURBIDITY_EROSION": "Cảnh báo dòng chảy: Đóng van lấy nước vào nhà máy cấp nước sinh hoạt để tránh nghẽn màng lọc.",
        "DEFAULT": "Cử đoàn kiểm tra hiện trường tại trạm quan trắc; xác nhận lại kết quả đo bằng thiết bị cầm tay.",
    }

    def __init__(
        self,
        kafka_bootstrap: str | None = None,
        alerts_topic: str = "river.alerts.critical",
    ):
        self.kafka_bootstrap = kafka_bootstrap or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
        self.alerts_topic = alerts_topic
        self._producer = None

    def _get_producer(self):
        """Lazy initializer for Kafka Producer."""
        if self._producer is None and KafkaProducer is not None:
            try:
                self._producer = KafkaProducer(
                    bootstrap_servers=self.kafka_bootstrap,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    acks=1,
                    retries=3,
                )
            except (KafkaError, OSError, ConnectionError) as e:
                logger.warning("Could not connect to Kafka broker (%s) for alerting: %s", self.kafka_bootstrap, e)
        return self._producer

    def determine_recommended_action(self, reasons: list[str]) -> str:
        """Selects actionable operational advice based on the detected incident."""
        text = " ".join(reasons).upper()
        if "OXY" in text or "DO" in text:
            return self.RECOMMENDED_ACTIONS["DISSOLVED_OXYGEN"]
        if "PH" in text or "AXIT" in text:
            return self.RECOMMENDED_ACTIONS["ACID_SPILL"]
        if "AMMONIA" in text:
            return self.RECOMMENDED_ACTIONS["AMMONIA_SEWAGE"]
        if "ĐỘ ĐỤC" in text or "TURBIDITY" in text:
            return self.RECOMMENDED_ACTIONS["TURBIDITY_EROSION"]
        return self.RECOMMENDED_ACTIONS["DEFAULT"]

    def create_alert(
        self,
        station_id: str,
        station_name: str,
        river_name: str,
        timestamp: str,
        severity: str,
        reasons: list[str],
        metrics: dict[str, float],
    ) -> CriticalAlertMessage:
        """Constructs an enriched, structured alert object."""
        action = self.determine_recommended_action(reasons)
        alert_type = (
            "TOXIC_DISCHARGE" if any("AMMONIA" in r.upper() or "PH" in r.upper() for r in reasons) else "ANOMALY_SURGE"
        )

        return CriticalAlertMessage(
            alert_id=str(uuid.uuid4()),
            station_id=station_id,
            station_name=station_name,
            river_name=river_name,
            timestamp=timestamp,
            severity=severity,
            alert_type=alert_type,
            reasons=reasons,
            metrics=metrics,
            recommended_action=action,
        )

    def dispatch_alert(self, alert: CriticalAlertMessage) -> bool:
        """Publishes the alert to Kafka and logs to alert stream."""
        logger.warning(
            "🚨 CRITICAL ALERT [%s] Station %s (%s): %s",
            alert.severity,
            alert.station_id,
            alert.river_name,
            "; ".join(alert.reasons),
        )

        producer = self._get_producer()
        if producer:
            try:
                producer.send(
                    self.alerts_topic,
                    key=alert.station_id.encode("utf-8"),
                    value=alert.to_dict(),
                )
                producer.flush()
                logger.info("Dispatched alert %s to Kafka topic %s.", alert.alert_id, self.alerts_topic)
                return True
            except (KafkaError, OSError, ConnectionError) as e:
                logger.error("Failed to publish alert to Kafka: %s", e)
        return False
