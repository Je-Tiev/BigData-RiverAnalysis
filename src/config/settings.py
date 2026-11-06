"""Centralized Platform Configuration and Settings.

Loads configurations from environment variables or .env files with sensible
production defaults, ensuring no hardcoded IPs or secrets in application logic.
"""

import os
from dataclasses import dataclass, field


@dataclass
class KafkaConfig:
    """Kafka Broker and Topic Configuration."""

    bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    raw_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_RAW", "river.sensors.raw"))
    dlq_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_DLQ", "river.sensors.dlq"))
    alerts_topic: str = field(default_factory=lambda: os.getenv("KAFKA_TOPIC_ALERTS", "river.alerts.critical"))
    consumer_group: str = field(default_factory=lambda: os.getenv("KAFKA_CONSUMER_GROUP", "river_quality_stream_group"))


@dataclass
class MinIOConfig:
    """MinIO / S3 Object Storage Configuration for Lakehouse."""

    endpoint: str = field(default_factory=lambda: os.getenv("MINIO_ENDPOINT", "localhost:9000"))
    access_key: str = field(default_factory=lambda: os.getenv("MINIO_ACCESS_KEY", "admin"))
    secret_key: str = field(default_factory=lambda: os.getenv("MINIO_SECRET_KEY", "password123"))
    bucket: str = field(default_factory=lambda: os.getenv("MINIO_BUCKET", "water-quality-lakehouse"))
    secure: bool = field(default_factory=lambda: os.getenv("MINIO_SECURE", "false").lower() == "true")

    @property
    def bronze_path(self) -> str:
        return f"s3a://{self.bucket}/bronze"

    @property
    def silver_path(self) -> str:
        return f"s3a://{self.bucket}/silver"

    @property
    def gold_path(self) -> str:
        return f"s3a://{self.bucket}/gold"


@dataclass
class InfluxDBConfig:
    """InfluxDB 2.x Time-Series Storage Configuration."""

    url: str = field(default_factory=lambda: os.getenv("INFLUXDB_URL", "http://localhost:8086"))
    token: str = field(default_factory=lambda: os.getenv("INFLUXDB_TOKEN", "my-super-secret-auth-token"))
    org: str = field(default_factory=lambda: os.getenv("INFLUXDB_ORG", "water-quality"))
    bucket: str = field(default_factory=lambda: os.getenv("INFLUXDB_BUCKET", "river-data"))


@dataclass
class SparkConfig:
    """Spark Streaming and Master Configuration."""

    master: str = field(default_factory=lambda: os.getenv("SPARK_MASTER", "local[*]"))
    app_name: str = field(default_factory=lambda: os.getenv("SPARK_APP_NAME", "RiverQualityStream"))
    checkpoint_base: str = field(
        default_factory=lambda: os.getenv("SPARK_CHECKPOINT_BASE", "/opt/spark-data/checkpoints")
    )
    trigger_seconds: int = field(default_factory=lambda: int(os.getenv("SPARK_TRIGGER_SECONDS", "10")))
    watermark_duration: str = field(default_factory=lambda: os.getenv("SPARK_WATERMARK_DURATION", "10 minutes"))


@dataclass
class SimulatorConfig:
    """IoT River Station Simulator Configuration."""

    delay_seconds: float = field(default_factory=lambda: float(os.getenv("SIMULATOR_DELAY_SECONDS", "1.0")))
    anomaly_probability: float = field(default_factory=lambda: float(os.getenv("SIMULATOR_ANOMALY_PROB", "0.05")))
    active_station_count: int = field(default_factory=lambda: int(os.getenv("SIMULATOR_STATION_COUNT", "10")))


@dataclass
class AppConfig:
    """Master Application Configuration."""

    kafka: KafkaConfig = field(default_factory=KafkaConfig)
    minio: MinIOConfig = field(default_factory=MinIOConfig)
    influxdb: InfluxDBConfig = field(default_factory=InfluxDBConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    simulator: SimulatorConfig = field(default_factory=SimulatorConfig)


# Global Config Singleton
config = AppConfig()
