"""River Water Quality Data Contracts and Schema Definitions.

Provides standardized schemas, physical boundary constants, and validation
functions for hydrological monitoring stations along river basins.
Standardizes column names (snake_case) and eliminates character encoding bugs.
"""

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Schema Version
SCHEMA_VERSION: str = "1.2.0"

# Default Kafka Topics
DEFAULT_TOPIC_SENSORS_RAW: str = "river.sensors.raw"
DEFAULT_TOPIC_SENSORS_DLQ: str = "river.sensors.dlq"
DEFAULT_TOPIC_ALERTS: str = "river.alerts.critical"

# Physical and Environmental Plausibility Limits
# Values outside these ranges represent physical sensor faults or transmission corruption
PHYSICAL_LIMITS: Dict[str, Tuple[float, float]] = {
    "temperature": (-5.0, 55.0),          # °C: typical fresh water range
    "ph": (0.0, 14.0),                    # pH scale standard
    "dissolved_oxygen": (0.0, 25.0),       # mg/L: saturation limits
    "conductivity": (0.0, 10000.0),       # µS/cm: freshwater conductivity
    "ammonia": (0.0, 50.0),               # mg/L as N
    "bod5": (0.0, 150.0),                 # mg/L: Biochemical Oxygen Demand 5-day
    "chloride": (0.0, 5000.0),            # mg/L
    "total_hardness": (0.0, 2000.0),      # mg/L as CaCO3
    "turbidity_ntu": (0.0, 2000.0),       # NTU (Nephelometric Turbidity Units)
    "flow_rate_m3s": (0.0, 50000.0),      # m³/s: river discharge
}

# Critical Safety Thresholds (QCVN 08-MT:2015/BTNMT - National Technical Regulation on Surface Water Quality)
CRITICAL_THRESHOLDS: Dict[str, Dict[str, float]] = {
    "dissolved_oxygen": {"min": 4.0, "severity": "HIGH"},     # DO < 4.0 mg/L threatens aquatic life
    "ph": {"min": 5.5, "max": 9.0, "severity": "HIGH"},       # pH outside 5.5-9.0 is hazardous
    "ammonia": {"max": 1.0, "severity": "HIGH"},             # Ammonia > 1.0 mg/L indicates organic waste
    "bod5": {"max": 15.0, "severity": "MEDIUM"},             # BOD > 15 mg/L indicates organic pollution
}

# Reference Stations across Major River Basins (Vietnam)
REFERENCE_STATIONS: List[Dict[str, Any]] = [
    {
        "station_id": "ST_HONG_LB_01",
        "station_name": "Trạm Cầu Long Biên",
        "river_name": "Sông Hồng",
        "basin_name": "Lưu vực sông Hồng - Thái Bình",
        "latitude": 21.0425,
        "longitude": 105.8614,
        "elevation_m": 12.5,
    },
    {
        "station_id": "ST_DA_HB_02",
        "station_name": "Trạm Hạ lưu Thủy điện Hòa Bình",
        "river_name": "Sông Đà",
        "basin_name": "Lưu vực sông Hồng - Thái Bình",
        "latitude": 20.8172,
        "longitude": 105.3218,
        "elevation_m": 22.0,
    },
    {
        "station_id": "ST_LO_VT_03",
        "station_name": "Trạm Ngã ba sông Lô - Việt Trì",
        "river_name": "Sông Lô",
        "basin_name": "Lưu vực sông Hồng - Thái Bình",
        "latitude": 21.2981,
        "longitude": 105.4389,
        "elevation_m": 16.0,
    },
    {
        "station_id": "ST_HUONG_TP_04",
        "station_name": "Trạm Cầu Tràng Tiền",
        "river_name": "Sông Hương",
        "basin_name": "Lưu vực sông Hương",
        "latitude": 16.4687,
        "longitude": 107.5909,
        "elevation_m": 5.0,
    },
    {
        "station_id": "ST_HAN_DN_05",
        "station_name": "Trạm Cầu Rồng",
        "river_name": "Sông Hàn",
        "basin_name": "Lưu vực sông Vu Gia - Thu Bồn",
        "latitude": 16.0610,
        "longitude": 108.2272,
        "elevation_m": 3.0,
    },
    {
        "station_id": "ST_SAIGON_BC_06",
        "station_name": "Trạm Bến Bạch Đằng",
        "river_name": "Sông Sài Gòn",
        "basin_name": "Lưu vực sông Đồng Nai",
        "latitude": 10.7725,
        "longitude": 106.7067,
        "elevation_m": 2.0,
    },
    {
        "station_id": "ST_DONGNAI_CL_07",
        "station_name": "Trạm Cát Lái",
        "river_name": "Sông Đồng Nai",
        "basin_name": "Lưu vực sông Đồng Nai",
        "latitude": 10.7583,
        "longitude": 106.7925,
        "elevation_m": 2.5,
    },
    {
        "station_id": "ST_TIEN_MYTHO_08",
        "station_name": "Trạm Mỹ Tho",
        "river_name": "Sông Tiền",
        "basin_name": "Lưu vực sông Cửu Long",
        "latitude": 10.3544,
        "longitude": 106.3653,
        "elevation_m": 1.5,
    },
    {
        "station_id": "ST_HAU_CT_09",
        "station_name": "Trạm Cầu Cần Thơ",
        "river_name": "Sông Hậu",
        "basin_name": "Lưu vực sông Cửu Long",
        "latitude": 10.0289,
        "longitude": 105.7876,
        "elevation_m": 1.8,
    },
    {
        "station_id": "ST_MA_TH_10",
        "station_name": "Trạm Cầu Hàm Rồng",
        "river_name": "Sông Mã",
        "basin_name": "Lưu vực sông Mã",
        "latitude": 19.8336,
        "longitude": 105.7722,
        "elevation_m": 8.0,
    },
]


@dataclass
class RiverSensorReading:
    """Standardized representation of a single telemetry record from a river station."""

    station_id: str
    station_name: str
    river_name: str
    basin_name: str
    latitude: float
    longitude: float
    timestamp: str  # ISO 8601 format: YYYY-MM-DDTHH:MM:SSZ
    temperature: float
    ph: float
    dissolved_oxygen: float
    conductivity: float
    ammonia: float
    bod5: float
    chloride: float
    total_hardness: float
    turbidity_ntu: float
    flow_rate_m3s: float
    ccme_score: Optional[float] = None
    quality_label: Optional[str] = None
    is_anomaly: bool = False
    anomaly_reason: Optional[str] = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> Dict[str, Any]:
        """Convert reading instance to serialized dictionary."""
        return {
            "schema_version": self.schema_version,
            "station_id": self.station_id,
            "station_name": self.station_name,
            "river_name": self.river_name,
            "basin_name": self.basin_name,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "timestamp": self.timestamp,
            "temperature": self.temperature,
            "ph": self.ph,
            "dissolved_oxygen": self.dissolved_oxygen,
            "conductivity": self.conductivity,
            "ammonia": self.ammonia,
            "bod5": self.bod5,
            "chloride": self.chloride,
            "total_hardness": self.total_hardness,
            "turbidity_ntu": self.turbidity_ntu,
            "flow_rate_m3s": self.flow_rate_m3s,
            "ccme_score": self.ccme_score,
            "quality_label": self.quality_label,
            "is_anomaly": self.is_anomaly,
            "anomaly_reason": self.anomaly_reason,
        }


def validate_sensor_payload(payload: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """Validate a raw payload against contract constraints and physical plausibility.

    Returns:
        Tuple of (is_valid: bool, error_message: Optional[str]).
    """
    # 1. Required Top-level Identifiers
    required_fields = ["station_id", "timestamp", "temperature", "ph", "dissolved_oxygen"]
    for f in required_fields:
        if f not in payload or payload[f] is None:
            return False, f"Missing required field: '{f}'"

    # 2. Timestamp Format Check
    ts_str = str(payload.get("timestamp"))
    try:
        # Support ISO 8601 variations (with or without Z)
        ts_clean = ts_str.replace("Z", "+00:00") if ts_str.endswith("Z") else ts_str
        datetime.fromisoformat(ts_clean)
    except Exception:
        # Fallback check for YYYY-MM-DD HH:MM:SS
        try:
            datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")
        except Exception:
            return False, f"Invalid timestamp format: '{ts_str}'. Expected ISO-8601 or 'YYYY-MM-DD HH:MM:SS'"

    # 3. Numeric Plausibility Range Checks
    for param, (low, high) in PHYSICAL_LIMITS.items():
        if param in payload and payload[param] is not None:
            try:
                val = float(payload[param])
                if val < low or val > high:
                    return False, f"Parameter '{param}' value {val} is outside physical bounds [{low}, {high}]"
            except (ValueError, TypeError):
                return False, f"Parameter '{param}' must be a valid float, got: {payload[param]}"

    return True, None


def get_spark_schema():
    """Returns PySpark StructType schema corresponding to the canonical sensor contract.

    Lazy-loads pyspark types to allow using this module in environments where
    PySpark is not locally installed.
    """
    try:
        from pyspark.sql.types import (
            BooleanType,
            DoubleType,
            StringType,
            StructField,
            StructType,
        )

        return StructType([
            StructField("schema_version", StringType(), True),
            StructField("station_id", StringType(), False),
            StructField("station_name", StringType(), True),
            StructField("river_name", StringType(), True),
            StructField("basin_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("timestamp", StringType(), False),
            StructField("temperature", DoubleType(), True),
            StructField("ph", DoubleType(), True),
            StructField("dissolved_oxygen", DoubleType(), True),
            StructField("conductivity", DoubleType(), True),
            StructField("ammonia", DoubleType(), True),
            StructField("bod5", DoubleType(), True),
            StructField("chloride", DoubleType(), True),
            StructField("total_hardness", DoubleType(), True),
            StructField("turbidity_ntu", DoubleType(), True),
            StructField("flow_rate_m3s", DoubleType(), True),
            StructField("ccme_score", DoubleType(), True),
            StructField("quality_label", StringType(), True),
            StructField("is_anomaly", BooleanType(), True),
            StructField("anomaly_reason", StringType(), True),
        ])
    except ImportError as e:
        raise ImportError("pyspark is required to construct get_spark_schema()") from e
