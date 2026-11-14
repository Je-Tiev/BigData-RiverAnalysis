"""Telemetry Service managing multi-station hydrological snapshots and time series."""

from datetime import UTC, datetime, timedelta
from typing import Any

from src.contracts.river_schema import REFERENCE_STATIONS
from src.producers.river_simulator import RiverTelemetryGenerator


class TelemetryService:
    """Core telemetry data provider for 10 national river monitoring stations."""

    _instance: "TelemetryService | None" = None

    def __init__(self) -> None:
        self.generator = RiverTelemetryGenerator()

    @classmethod
    def get_instance(cls) -> "TelemetryService":
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def get_current_network_state(self, force_refresh: bool = False) -> list[dict[str, Any]]:
        """Generates the latest state snapshot across all 10 river stations.

        Guarantees the presence of required test keys:
        'station_id', 'station_name', 'river_name', 'basin_name',
        'latitude', 'longitude', 'temperature', 'ph', 'dissolved_oxygen',
        'ccme_score', 'quality_label', 'is_anomaly'.
        """
        now = datetime.now(UTC)
        network_data: list[dict[str, Any]] = []
        for st_meta in REFERENCE_STATIONS:
            reading = self.generator.generate_single_reading(st_meta, override_timestamp=now)
            d = reading.to_dict()
            network_data.append(d)
        return network_data

    def generate_recent_series(self, station_meta: dict[str, Any], hours: int = 24) -> list[dict[str, Any]]:
        """Generates a realistic historical hourly time series for a station.

        Guarantees:
        - Exactly (hours + 1) points (e.g. 25 points for 24 hours).
        - Presence of 'dissolved_oxygen', 'timestamp', and DO > 0.0.
        """
        series: list[dict[str, Any]] = []
        base_time = datetime.now(UTC) - timedelta(hours=hours)
        for h in range(hours + 1):
            ts = base_time + timedelta(hours=h)
            reading = self.generator.generate_single_reading(station_meta, override_timestamp=ts)
            series.append(reading.to_dict())
        return series

    def get_station_metadata(self, station_id: str) -> dict[str, Any] | None:
        """Looks up static station metadata by station_id."""
        for st in REFERENCE_STATIONS:
            if st["station_id"] == station_id:
                return st
        return None

    def get_all_stations_metadata(self) -> list[dict[str, Any]]:
        """Returns the static list of all 10 reference stations."""
        return list(REFERENCE_STATIONS)
