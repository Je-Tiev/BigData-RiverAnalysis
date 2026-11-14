"""River Water Quality Data Contracts and Schema Definitions.

Provides standardized schemas, physical boundary constants, and validation
functions for hydrological monitoring stations along river basins.
Standardizes column names (snake_case) and eliminates character encoding bugs.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

# Schema Version
SCHEMA_VERSION: str = "1.3.0"

# Default Kafka Topics
DEFAULT_TOPIC_SENSORS_RAW: str = "river.sensors.raw"
DEFAULT_TOPIC_SENSORS_DLQ: str = "river.sensors.dlq"
DEFAULT_TOPIC_ALERTS: str = "river.alerts.critical"

# Physical and Environmental Plausibility Limits
# Values outside these ranges represent physical sensor faults or transmission corruption
PHYSICAL_LIMITS: dict[str, tuple[float, float]] = {
    # 10 Tham số cốt lõi hiện hữu (Giữ nguyên tuyệt đối)
    "temperature": (-5.0, 55.0),  # °C: typical fresh water range
    "ph": (0.0, 14.0),  # pH scale standard
    "dissolved_oxygen": (0.0, 25.0),  # mg/L: saturation limits
    "conductivity": (0.0, 10000.0),  # µS/cm: freshwater conductivity
    "ammonia": (0.0, 50.0),  # mg/L as N
    "bod5": (0.0, 150.0),  # mg/L: Biochemical Oxygen Demand 5-day
    "chloride": (0.0, 5000.0),  # mg/L
    "total_hardness": (0.0, 2000.0),  # mg/L as CaCO3
    "turbidity_ntu": (0.0, 2000.0),  # NTU (Nephelometric Turbidity Units)
    "flow_rate_m3s": (0.0, 50000.0),  # m³/s: river discharge
    # 8 Tham số mở rộng QCVN 08:2023/BTNMT mới
    "cod": (0.0, 500.0),  # mg/L: Chemical Oxygen Demand
    "tss": (0.0, 1000.0),  # mg/L: Total Suspended Solids
    "nitrate_no3": (0.0, 100.0),  # mg N/L: Nitrate as Nitrogen
    "phosphate_po4": (0.0, 20.0),  # mg P/L: ortho-Phosphate as Phosphorus
    "coliform": (0.0, 1000000.0),  # MPN/100mL: Total Coliform
    "salinity": (0.0, 50.0),  # ppt (‰): Salinity
    "alkalinity": (0.0, 1000.0),  # mg/L as CaCO3: Total Alkalinity
    "true_colour": (0.0, 500.0),  # Pt-Co: Platinum-Cobalt color scale
}

# Critical Safety Thresholds (QCVN 08-MT:2015/BTNMT - National Technical Regulation on Surface Water Quality)
CRITICAL_THRESHOLDS: dict[str, dict[str, Any]] = {
    "dissolved_oxygen": {"min": 4.0, "severity": "HIGH"},  # DO < 4.0 mg/L threatens aquatic life
    "ph": {"min": 5.5, "max": 9.0, "severity": "HIGH"},  # pH outside 5.5-9.0 is hazardous
    "ammonia": {"max": 1.0, "severity": "HIGH"},  # Ammonia > 1.0 mg/L indicates organic waste
    "bod5": {"max": 15.0, "severity": "MEDIUM"},  # BOD > 15 mg/L indicates organic pollution
    "cod": {"max": 50.0, "severity": "MEDIUM"},
    "coliform": {"max": 10000.0, "severity": "HIGH"},
    "salinity": {"max": 4.0, "severity": "HIGH"},
}

# Reference Stations across Major River Basins (Vietnam)
REFERENCE_STATIONS: list[dict[str, Any]] = [
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

    # 1. Core Identifiers & Mandatory Physical Telemetry (NO DEFAULT - 10 fields)
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

    # 2. Legacy Optional Sensor Readings (Defaults matching v1.2.0)
    conductivity: float = 300.0
    ammonia: float = 0.05
    bod5: float = 2.0
    chloride: float = 25.0
    total_hardness: float = 120.0
    turbidity_ntu: float = 30.0
    flow_rate_m3s: float = 1000.0

    # 3. New QCVN 08:2023 Expanded Fields (Strictly Optional[float] = None)
    cod: float | None = None
    tss: float | None = None
    nitrate_no3: float | None = None
    phosphate_po4: float | None = None
    coliform: float | None = None
    salinity: float | None = None
    alkalinity: float | None = None
    true_colour: float | None = None

    # 4. Analytical Scores, Indices & Operational Flags
    ccme_score: float | None = None
    quality_label: str | None = None
    vn_wqi_score: float | None = None
    vn_wqi_label: str | None = None
    qcvn_tier: str | None = None
    is_anomaly: bool = False
    anomaly_reason: str | None = None
    schema_version: str = SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
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
            "cod": self.cod,
            "tss": self.tss,
            "nitrate_no3": self.nitrate_no3,
            "phosphate_po4": self.phosphate_po4,
            "coliform": self.coliform,
            "salinity": self.salinity,
            "alkalinity": self.alkalinity,
            "true_colour": self.true_colour,
            "ccme_score": self.ccme_score,
            "quality_label": self.quality_label,
            "vn_wqi_score": self.vn_wqi_score,
            "vn_wqi_label": self.vn_wqi_label,
            "qcvn_tier": self.qcvn_tier,
            "is_anomaly": self.is_anomaly,
            "anomaly_reason": self.anomaly_reason,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "RiverSensorReading":
        """Instantiate RiverSensorReading from a dictionary payload."""
        return cls(
            station_id=data["station_id"],
            station_name=data.get("station_name", ""),
            river_name=data.get("river_name", ""),
            basin_name=data.get("basin_name", ""),
            latitude=float(data.get("latitude", 0.0)),
            longitude=float(data.get("longitude", 0.0)),
            timestamp=data["timestamp"],
            temperature=float(data["temperature"]),
            ph=float(data["ph"]),
            dissolved_oxygen=float(data["dissolved_oxygen"]),
            conductivity=float(data.get("conductivity", 300.0)),
            ammonia=float(data.get("ammonia", 0.05)),
            bod5=float(data.get("bod5", 2.0)),
            chloride=float(data.get("chloride", 25.0)),
            total_hardness=float(data.get("total_hardness", 120.0)),
            turbidity_ntu=float(data.get("turbidity_ntu", 30.0)),
            flow_rate_m3s=float(data.get("flow_rate_m3s", 1000.0)),
            cod=float(data["cod"]) if data.get("cod") is not None else None,
            tss=float(data["tss"]) if data.get("tss") is not None else None,
            nitrate_no3=float(data["nitrate_no3"]) if data.get("nitrate_no3") is not None else None,
            phosphate_po4=float(data["phosphate_po4"]) if data.get("phosphate_po4") is not None else None,
            coliform=float(data["coliform"]) if data.get("coliform") is not None else None,
            salinity=float(data["salinity"]) if data.get("salinity") is not None else None,
            alkalinity=float(data["alkalinity"]) if data.get("alkalinity") is not None else None,
            true_colour=float(data["true_colour"]) if data.get("true_colour") is not None else None,
            ccme_score=float(data["ccme_score"]) if data.get("ccme_score") is not None else None,
            quality_label=data.get("quality_label"),
            vn_wqi_score=float(data["vn_wqi_score"]) if data.get("vn_wqi_score") is not None else None,
            vn_wqi_label=data.get("vn_wqi_label"),
            qcvn_tier=data.get("qcvn_tier"),
            is_anomaly=bool(data.get("is_anomaly", False)),
            anomaly_reason=data.get("anomaly_reason"),
            schema_version=data.get("schema_version", SCHEMA_VERSION),
        )


def validate_sensor_payload(payload: dict[str, Any]) -> tuple[bool, str | None]:
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
    except (ValueError, TypeError):
        # Fallback check for YYYY-MM-DD HH:MM:SS
        try:
            datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC)
        except (ValueError, TypeError):
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

        return StructType(
            [
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
                # 8 Expanded Nullable Fields
                StructField("cod", DoubleType(), True),
                StructField("tss", DoubleType(), True),
                StructField("nitrate_no3", DoubleType(), True),
                StructField("phosphate_po4", DoubleType(), True),
                StructField("coliform", DoubleType(), True),
                StructField("salinity", DoubleType(), True),
                StructField("alkalinity", DoubleType(), True),
                StructField("true_colour", DoubleType(), True),
                # Analytical Fields
                StructField("ccme_score", DoubleType(), True),
                StructField("quality_label", StringType(), True),
                StructField("vn_wqi_score", DoubleType(), True),
                StructField("vn_wqi_label", StringType(), True),
                StructField("qcvn_tier", StringType(), True),
                StructField("is_anomaly", BooleanType(), True),
                StructField("anomaly_reason", StringType(), True),
            ]
        )
    except ImportError as e:
        raise ImportError("pyspark is required to construct get_spark_schema()") from e
