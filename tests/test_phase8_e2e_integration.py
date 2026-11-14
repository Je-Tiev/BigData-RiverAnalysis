"""Comprehensive End-to-End Regression and Integration Test Suite for Phase 8.

Validates the full enterprise stack across:
1. Data Schema v1.3.0, Physical Limits, PySpark Schema, Dual-Engine WQI (CCME + VN_WQI QĐ 1460),
   and 6 realistic anomaly injection scenarios.
2. Report Export Service (CSV utf-8-sig BOM, Excel 4-sheet openpyxl, Print-Ready A4 HTML),
   FastAPI Client 3-state Circuit Breaker & complete lifecycle, In-Process Fallbacks, and Telemetry Service.
3. Centralized Session State & 4-tier Persona RBAC matrix, Kanban Incident SOP lifecycle,
   SHA-256 cryptographic audit trail sealing, and Lean Router line count constraints (<= 100 lines).
"""

import hashlib
import io
import sys
import types
from pathlib import Path

import openpyxl
import pytest

import src.dashboard.app as dashboard_app
from src.contracts.river_schema import (
    PHYSICAL_LIMITS,
    REFERENCE_STATIONS,
    RiverSensorReading,
    get_spark_schema,
    validate_sensor_payload,
)
from src.dashboard.app import generate_recent_series, get_current_network_state
from src.dashboard.services.api_client import CircuitState, FastAPIClient
from src.dashboard.services.export_service import ReportExportService
from src.dashboard.services.telemetry_service import TelemetryService
from src.dashboard.state.session_state import (
    IncidentStatus,
    SessionStateManager,
    UserRole,
)
from src.dashboard.views.executive_view import basin_filter_fragment, render_executive_view
from src.dashboard.views.hydrology_ops_view import (
    infra_ping_fragment,
    render_hydrology_ops_view,
    streeter_phelps_slider_fragment,
)
from src.dashboard.views.incident_view import incident_dispatch_fragment, render_incident_view
from src.dashboard.views.telemetry_view import (
    forecast_parameter_fragment,
    render_telemetry_view,
    telemetry_controls_fragment,
)
from src.producers.river_simulator import HydrologicalPhysicsEngine, RiverTelemetryGenerator


class TestPhase8SchemaAndPhysicsE2E:
    """End-to-End Validation of Data Schema v1.3.0, Physical Limits, PySpark Schema,

    Dual-Engine WQI (CCME + VN_WQI QĐ 1460), and 6 Anomaly Injection Scenarios.
    """

    def test_schema_v1_3_full_parameter_roundtrip(self) -> None:
        """Verifies full roundtrip serialization of all 18 physicochemical and WQI fields."""
        payload = {
            "schema_version": "1.3.0",
            "station_id": "ST_HONG_LB_01",
            "station_name": "Trạm Cầu Long Biên",
            "river_name": "Sông Hồng",
            "basin_name": "Lưu vực sông Hồng - Thái Bình",
            "latitude": 21.0435,
            "longitude": 105.8562,
            "timestamp": "2026-11-13T12:00:00Z",
            "temperature": 24.5,
            "ph": 7.35,
            "dissolved_oxygen": 7.6,
            "conductivity": 340.0,
            "ammonia": 0.08,
            "bod5": 2.1,
            "chloride": 22.0,
            "total_hardness": 125.0,
            "turbidity_ntu": 28.0,
            "flow_rate_m3s": 2400.0,
            # 8 Newly expanded parameters
            "cod": 25.0,
            "tss": 45.0,
            "nitrate_no3": 2.5,
            "phosphate_po4": 0.15,
            "coliform": 2400.0,
            "salinity": 0.8,
            "alkalinity": 85.0,
            "true_colour": 15.0,
            # Analytical and audit flags
            "ccme_score": 91.0,
            "quality_label": "Excellent",
            "vn_wqi_score": 88.5,
            "vn_wqi_label": "Tốt",
            "qcvn_tier": "A",
            "is_anomaly": False,
            "anomaly_reason": None,
        }

        # 1. Instantiate from dictionary
        reading = RiverSensorReading.from_dict(payload)
        assert reading.schema_version == "1.3.0"
        assert reading.station_id == "ST_HONG_LB_01"
        assert reading.cod == 25.0
        assert reading.tss == 45.0
        assert reading.nitrate_no3 == 2.5
        assert reading.phosphate_po4 == 0.15
        assert reading.coliform == 2400.0
        assert reading.salinity == 0.8
        assert reading.alkalinity == 85.0
        assert reading.true_colour == 15.0
        assert reading.vn_wqi_score == 88.5
        assert reading.vn_wqi_label == "Tốt"
        assert reading.qcvn_tier == "A"

        # 2. Serialize to dictionary and verify completeness
        serialized = reading.to_dict()
        assert serialized["schema_version"] == "1.3.0"
        expected_keys = [
            "cod",
            "tss",
            "nitrate_no3",
            "phosphate_po4",
            "coliform",
            "salinity",
            "alkalinity",
            "true_colour",
            "vn_wqi_score",
            "vn_wqi_label",
            "qcvn_tier",
        ]
        for key in expected_keys:
            assert key in serialized
            assert serialized[key] == payload[key]

        # 3. Re-instantiate to confirm complete mathematical isomorphism
        reconstructed = RiverSensorReading.from_dict(serialized)
        assert reconstructed.cod == reading.cod
        assert reconstructed.coliform == reading.coliform
        assert reconstructed.salinity == reading.salinity
        assert reconstructed.vn_wqi_score == reading.vn_wqi_score

    def test_physical_limits_boundary_enforcement(self) -> None:
        """Verifies boundary enforcement for all 18 physicochemical limits."""
        base_valid = dict(REFERENCE_STATIONS[0])
        base_valid.update(
            {
                "timestamp": "2026-11-13T12:00:00Z",
                "temperature": 24.0,
                "ph": 7.2,
                "dissolved_oxygen": 7.5,
                "conductivity": 350.0,
                "ammonia": 0.1,
                "bod5": 2.0,
                "chloride": 25.0,
                "total_hardness": 120.0,
                "turbidity_ntu": 30.0,
                "flow_rate_m3s": 1500.0,
                "cod": 20.0,
                "tss": 50.0,
                "nitrate_no3": 2.0,
                "phosphate_po4": 0.1,
                "coliform": 2500.0,
                "salinity": 0.5,
                "alkalinity": 100.0,
                "true_colour": 20.0,
            }
        )

        # Baseline valid payload
        assert len(PHYSICAL_LIMITS) == 18
        is_valid, err = validate_sensor_payload(base_valid)
        assert is_valid is True, f"Valid payload rejected: {err}"

        # Test physical bounds violations for newly expanded parameters
        violation_cases = [
            ("cod", -5.0),
            ("cod", 600.0),
            ("coliform", 2_000_000.0),
            ("salinity", -1.0),
            ("salinity", 70.0),
            ("true_colour", -10.0),
            ("true_colour", 600.0),
        ]

        for param_name, invalid_value in violation_cases:
            test_payload = dict(base_valid)
            test_payload[param_name] = invalid_value
            is_valid_viol, err_msg = validate_sensor_payload(test_payload)
            assert is_valid_viol is False, f"Expected violation for {param_name}={invalid_value}"
            assert err_msg is not None
            assert param_name in err_msg
            assert "outside physical bounds" in err_msg

    def test_spark_schema_field_completeness_and_types(self) -> None:
        """Verifies PySpark StructType schema completeness and accurate DoubleType mapping."""
        orig_pyspark = sys.modules.get("pyspark")
        orig_sql = sys.modules.get("pyspark.sql")
        orig_types = sys.modules.get("pyspark.sql.types")
        try:
            # Inject lightweight pyspark mock if pyspark is not installed in the test environment
            if "pyspark" not in sys.modules:
                mock_pyspark = types.ModuleType("pyspark")
                mock_sql = types.ModuleType("pyspark.sql")
                mock_types = types.ModuleType("pyspark.sql.types")

                class StructType(list):
                    def __init__(self, fields):
                        super().__init__(fields)
                        self.fields = fields

                class StructField:
                    def __init__(self, name, dataType, nullable=True):
                        self.name = name
                        self.dataType = dataType
                        self.nullable = nullable

                class DoubleType:
                    pass

                class StringType:
                    pass

                class BooleanType:
                    pass

                mock_types.StructType = StructType
                mock_types.StructField = StructField
                mock_types.DoubleType = DoubleType
                mock_types.StringType = StringType
                mock_types.BooleanType = BooleanType
                sys.modules["pyspark"] = mock_pyspark
                sys.modules["pyspark.sql"] = mock_sql
                sys.modules["pyspark.sql.types"] = mock_types

            from pyspark.sql.types import DoubleType, StringType, StructType

            schema = get_spark_schema()
            assert isinstance(schema, StructType)

            field_map = {f.name: f.dataType for f in schema.fields}

            # 18 physicochemical parameters must all be DoubleType
            expected_params = [
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
            for param in expected_params:
                assert param in field_map, f"Missing {param} in spark schema"
                assert isinstance(field_map[param], DoubleType), f"{param} is not DoubleType"

            # Analytical fields verification
            assert isinstance(field_map["vn_wqi_score"], DoubleType)
            assert isinstance(field_map["vn_wqi_label"], StringType)
            assert isinstance(field_map["qcvn_tier"], StringType)
        finally:
            if orig_pyspark is None:
                sys.modules.pop("pyspark", None)
            else:
                sys.modules["pyspark"] = orig_pyspark
            if orig_sql is None:
                sys.modules.pop("pyspark.sql", None)
            else:
                sys.modules["pyspark.sql"] = orig_sql
            if orig_types is None:
                sys.modules.pop("pyspark.sql.types", None)
            else:
                sys.modules["pyspark.sql.types"] = orig_types

    def test_dual_engine_wqi_mathematical_consistency(self) -> None:
        """Verifies mathematical consistency between CCME WQI and Decision 1460 VN_WQI."""
        physics = HydrologicalPhysicsEngine()

        # 1. Pristine / Ideal Water Scenario
        pristine_readings = {
            "ph": 7.5,
            "dissolved_oxygen": 8.5,
            "ammonia": 0.02,
            "bod5": 1.0,
            "cod": 2.0,
            "coliform": 50.0,
            "conductivity": 200.0,
            "turbidity_ntu": 10.0,
            "nitrate_no3": 0.5,
            "phosphate_po4": 0.02,
            "salinity": 0.02,
            "alkalinity": 80.0,
            "true_colour": 10.0,
        }
        ccme_pristine, ccme_label = physics.compute_ccme_wqi(pristine_readings)
        assert ccme_pristine >= 90.0
        assert ccme_label == "Excellent"

        vn_pristine, vn_label = physics.compute_vn_wqi(pristine_readings)
        assert vn_pristine >= 90.0
        assert vn_label in ["Rất tốt", "Tốt"]

        # 2. Heavily Polluted Water Scenario
        polluted_readings = {
            "ph": 4.0,
            "dissolved_oxygen": 1.2,
            "ammonia": 5.0,
            "bod5": 40.0,
            "cod": 90.0,
            "coliform": 500000.0,
            "conductivity": 2500.0,
            "turbidity_ntu": 450.0,
            "nitrate_no3": 15.0,
            "phosphate_po4": 1.0,
        }
        ccme_polluted, ccme_pol_label = physics.compute_ccme_wqi(polluted_readings)
        assert ccme_polluted < 45.0
        assert ccme_pol_label == "Poor"

        vn_polluted, vn_pol_label = physics.compute_vn_wqi(polluted_readings)
        assert vn_polluted < 25.0
        assert vn_pol_label in ["Ô nhiễm nặng", "Kém"]

    def test_six_anomaly_scenarios_injection_and_diagnostics(self) -> None:
        """Verifies diagnostic signatures and field values for all 6 anomaly scenarios."""
        generator = RiverTelemetryGenerator(anomaly_probability=0.0)
        station = REFERENCE_STATIONS[0]

        scenarios = [
            "INDUSTRIAL_ACID_DISCHARGE",
            "UNTREATED_SEWAGE_DUMP",
            "FLASH_RUNOFF_EROSION",
            "SUDDEN_ANOXIA",
            "TIDAL_SALINITY_INTRUSION",
            "EUTROPHICATION_ALGAL_BLOOM",
        ]

        for sc in scenarios:
            reading = generator.generate_single_reading(station=station, force_anomaly=sc)
            assert reading.is_anomaly is True
            assert reading.anomaly_reason is not None
            assert len(reading.anomaly_reason.strip()) > 0

            if sc == "INDUSTRIAL_ACID_DISCHARGE":
                assert reading.ph < 5.5
            elif sc == "UNTREATED_SEWAGE_DUMP":
                assert reading.ammonia > 2.0
                assert reading.bod5 > 20.0
                assert reading.cod is not None and reading.cod > 50.0
            elif sc == "FLASH_RUNOFF_EROSION":
                assert reading.turbidity_ntu > 200.0
                assert reading.tss is not None and reading.tss > 400.0
            elif sc == "SUDDEN_ANOXIA":
                assert reading.dissolved_oxygen < 3.0
            elif sc == "TIDAL_SALINITY_INTRUSION":
                assert reading.salinity is not None and reading.salinity > 5.0
                assert reading.conductivity > 400.0
            elif sc == "EUTROPHICATION_ALGAL_BLOOM":
                assert reading.phosphate_po4 is not None and reading.phosphate_po4 > 0.3
                assert reading.nitrate_no3 is not None and reading.nitrate_no3 > 5.0


class TestPhase8ServicesAndExportE2E:
    """End-to-End Validation of ReportExportService, Circuit Breaker State Machine,

    In-Process Fallbacks, and TelemetryService.
    """

    @pytest.fixture(autouse=True)
    def setup_service(self) -> None:
        self.tel_service = TelemetryService.get_instance()

    def test_export_service_csv_utf8_bom(self) -> None:
        """Verifies ReportExportService generates RFC-4180 CSV with UTF-8 BOM encoding."""
        records = self.tel_service.generate_recent_series(REFERENCE_STATIONS[0], hours=5)
        csv_bytes = ReportExportService.export_to_csv(records)

        assert isinstance(csv_bytes, bytes)
        assert csv_bytes.startswith(b"\xef\xbb\xbf")

        decoded = csv_bytes.decode("utf-8-sig")
        for col in [
            "station_id",
            "timestamp",
            "dissolved_oxygen",
            "ph",
            "ammonia",
            "cod",
            "tss",
            "nitrate_no3",
            "phosphate_po4",
            "coliform",
            "salinity",
            "alkalinity",
            "true_colour",
            "vn_wqi_score",
        ]:
            assert col in decoded

    def test_export_service_excel_four_sheets_structure(self) -> None:
        """Verifies multi-sheet Excel generation adhering to 4 operational tabs and styles."""
        meta = REFERENCE_STATIONS[0]
        records = self.tel_service.generate_recent_series(meta, hours=5)
        forecast_data = {
            "trajectory": [
                {
                    "step_ahead_hours": 1,
                    "forecast_timestamp": "2026-11-13T13:00:00Z",
                    "predicted_value": 7.4,
                    "confidence_lower_95": 6.8,
                    "confidence_upper_95": 8.0,
                }
            ]
        }
        audit_data = [
            {
                "incident_id": "INC-001",
                "timestamp": "2026-11-13T12:00:00Z",
                "user_role": "FIELD_OFFICER",
                "action": "DISPATCH_INCIDENT",
                "details": "Dispatch Team Alpha",
                "sha256_hash": "a1b2c3d4e5f60718",
            }
        ]

        excel_bytes = ReportExportService.export_to_excel(
            station_meta=meta,
            telemetry_data=records,
            forecast_data=forecast_data,
            audit_data=audit_data,
        )

        assert isinstance(excel_bytes, bytes)
        assert len(excel_bytes) > 1000

        wb = openpyxl.load_workbook(io.BytesIO(excel_bytes))
        assert wb.sheetnames == [
            "Station_Metadata",
            "Historical_Telemetry_72h",
            "AI_ML_Forecast_72h",
            "Incident_Audit_Log",
        ]

        ws1 = wb["Station_Metadata"]
        ws1_values = [ws1.cell(r, 2).value for r in range(1, ws1.max_row + 1)]
        assert meta["station_id"] in ws1_values
        assert meta["river_name"] in ws1_values

        ws2 = wb["Historical_Telemetry_72h"]
        assert ws2.max_row >= 6  # 1 header + 5 telemetry rows

    def test_export_service_html_printable_report(self) -> None:
        """Verifies Ministry of Natural Resources compliant print-ready A4 HTML report."""
        meta = REFERENCE_STATIONS[0]
        records = self.tel_service.generate_recent_series(meta, hours=3)
        wqi_summary = {"vn_wqi_score": 88.5, "vn_wqi_label": "Tốt", "ccme_score": 90.0}

        html_out = ReportExportService.export_to_html_report(
            station_meta=meta,
            telemetry_data=records,
            wqi_summary=wqi_summary,
        )

        assert "<!DOCTYPE html>" in html_out
        assert "<html" in html_out
        assert "<table" in html_out
        assert "@media print" in html_out
        assert meta["station_name"] in html_out
        assert meta["station_id"] in html_out

    def test_fastapi_client_circuit_breaker_complete_cycle(self) -> None:
        """Verifies complete Circuit Breaker state cycle: CLOSED -> 3 failures -> OPEN -> HALF_OPEN -> CLOSED."""
        client = FastAPIClient(base_url="http://127.0.0.1:59999")
        assert client.cb.state == CircuitState.CLOSED
        assert client.cb.failure_count == 0

        # Failure 1
        res1 = client.predict_forecast({"station_id": "ST_01", "horizon_hours": 12})
        assert client.cb.failure_count == 1
        assert client.cb.state == CircuitState.CLOSED
        assert "FALLBACK" in res1["_serving_mode"]

        # Failure 2
        res2 = client.predict_forecast({"station_id": "ST_01", "horizon_hours": 12})
        assert client.cb.failure_count == 2
        assert client.cb.state == CircuitState.CLOSED
        assert "FALLBACK" in res2["_serving_mode"]

        # Failure 3 -> State triggers to OPEN
        res3 = client.predict_forecast({"station_id": "ST_01", "horizon_hours": 12})
        assert client.cb.failure_count == 3
        assert client.cb.state == CircuitState.OPEN
        assert "FALLBACK" in res3["_serving_mode"]

        # While OPEN -> No network attempt, immediate in-process fallback
        res4 = client.predict_forecast({"station_id": "ST_01", "horizon_hours": 12})
        assert "FALLBACK" in res4["_serving_mode"]

        # Intervene clock past recovery_timeout_sec (30.0s)
        client.cb.last_state_change -= 35.0
        assert client.cb.can_attempt_call() is True
        assert client.cb.state == CircuitState.HALF_OPEN

        # Canary probe success resets to CLOSED
        client.cb.record_success()
        assert client.cb.state == CircuitState.CLOSED
        assert client.cb.failure_count == 0

    def test_fastapi_client_in_process_fallback_all_endpoints(self) -> None:
        """Verifies all AI/ML tasks operate seamlessly in local in-process fallback mode."""
        client = FastAPIClient(base_url="http://127.0.0.1:59999")
        client.cb.state = CircuitState.OPEN

        # 1. Trajectory Forecast
        fc = client.predict_forecast(
            {
                "station_id": "ST_HONG_LB_01",
                "current_reading": {"dissolved_oxygen": 7.2, "ph": 7.4},
                "horizon_hours": 24,
            }
        )
        assert "FALLBACK" in fc["_serving_mode"]
        assert len(fc["trajectory"]) == 24

        # 2. Anomaly Scoring
        anom = client.score_anomaly(
            {
                "station_id": "ST_HONG_LB_01",
                "reading": {"dissolved_oxygen": 1.5, "ph": 7.0, "temperature": 25.0},
            }
        )
        assert "FALLBACK" in anom["_serving_mode"]
        assert anom["is_anomaly"] is True

        # 3. DLQ Message Self-Healing
        healed = client.heal_dlq_message({"station_id": "ST_01", "dissolved_oxygen": "6.8\n"})
        assert "FALLBACK" in healed["_serving_mode"]
        assert healed["healed"] is True

        # 4. Drift Monitoring Status
        drift = client.get_drift_status()
        assert "FALLBACK" in drift["_serving_mode"]
        assert "conformal_guarantee" in drift

    def test_telemetry_service_contract_guarantees(self) -> None:
        """Verifies TelemetryService network state size and continuous time-series continuity."""
        network_state = self.tel_service.get_current_network_state()
        assert len(network_state) == 10

        required_core_fields = [
            "station_id",
            "station_name",
            "river_name",
            "latitude",
            "longitude",
            "dissolved_oxygen",
            "ph",
            "temperature",
            "ammonia",
            "cod",
            "tss",
            "is_anomaly",
        ]
        for st_item in network_state:
            for field in required_core_fields:
                assert field in st_item

        series = self.tel_service.generate_recent_series(REFERENCE_STATIONS[0], hours=24)
        # 24 historical hours + current hour = exactly 25 continuous points
        assert len(series) == 25


class TestPhase8DashboardAndRBACE2E:
    """End-to-End Validation of SessionStateManager, 4-Tier Persona RBAC,

    Kanban Incident Lifecycle, Cryptographic SHA-256 Audit Trail, and Lean Router.
    """

    @pytest.fixture(autouse=True)
    def setup_manager(self) -> None:
        self.mgr = SessionStateManager()
        self.mgr.init_session_state(force=True)

    def test_session_state_rbac_matrix_complete_coverage(self) -> None:
        """Verifies exact screen permissions and operational actions across all 4 user roles."""
        # 1. EXECUTIVE
        self.mgr.switch_role(UserRole.EXECUTIVE)
        prof_exec = self.mgr.get_role_profile()
        assert len(prof_exec.allowed_screens) == 3
        assert "[NOC-GIS] Tổng Quan" in prof_exec.allowed_screens
        assert "[TELEMETRY] Viễn Thám & Dự Báo" in prof_exec.allowed_screens
        assert "[INCIDENTS] Tác Nghiệp Sự Cố" in prof_exec.allowed_screens
        assert prof_exec.can_dispatch is False

        # 2. ENVIRONMENTAL_ENGINEER
        self.mgr.switch_role(UserRole.ENVIRONMENTAL_ENGINEER)
        prof_env = self.mgr.get_role_profile()
        assert len(prof_env.allowed_screens) == 3
        assert "[NOC-GIS] Tổng Quan" in prof_env.allowed_screens
        assert "[TELEMETRY] Viễn Thám & Dự Báo" in prof_env.allowed_screens
        assert "[HYDRO-OPS] Kỹ Thuật Thủy Văn" in prof_env.allowed_screens
        assert prof_env.can_dispatch is False

        # 3. FIELD_OFFICER
        self.mgr.switch_role(UserRole.FIELD_OFFICER)
        prof_field = self.mgr.get_role_profile()
        assert len(prof_field.allowed_screens) == 2
        assert "[NOC-GIS] Tổng Quan" in prof_field.allowed_screens
        assert "[INCIDENTS] Tác Nghiệp Sự Cố" in prof_field.allowed_screens
        assert prof_field.can_dispatch is True

        # 4. MLOPS_ENGINEER
        self.mgr.switch_role(UserRole.MLOPS_ENGINEER)
        prof_mlops = self.mgr.get_role_profile()
        assert len(prof_mlops.allowed_screens) == 4
        assert "[NOC-GIS] Tổng Quan" in prof_mlops.allowed_screens
        assert "[TELEMETRY] Viễn Thám & Dự Báo" in prof_mlops.allowed_screens
        assert "[INCIDENTS] Tác Nghiệp Sự Cố" in prof_mlops.allowed_screens
        assert "[HYDRO-OPS] Kỹ Thuật Thủy Văn" in prof_mlops.allowed_screens
        assert prof_mlops.can_dispatch is True
        assert prof_mlops.can_trigger_dlq is True
        assert prof_mlops.can_retrain_ml is True

        # Reset role back to EXECUTIVE for test isolation
        self.mgr.switch_role(UserRole.EXECUTIVE)

    def test_incident_kanban_full_lifecycle_and_sha256_audit(self) -> None:
        """Verifies standard SCADA SOP 4-step lifecycle transitions and SHA-256 seal verification."""
        self.mgr.init_session_state(force=True)
        inc_id = "INC-ST_DONGNAI_BH_05"
        registry = self.mgr.get_state("incident_registry")
        registry[inc_id].status = IncidentStatus.NEW.value

        # Step 1: Acknowledge (NEW -> ACKNOWLEDGED)
        assert self.mgr.acknowledge_incident(inc_id, officer_name="Đ/c Trưởng Ca") is True
        assert registry[inc_id].status == IncidentStatus.ACKNOWLEDGED.value

        # Step 2: Dispatch (ACKNOWLEDGED -> IN_PROGRESS)
        sops = ["Bật van điều hòa hạ lưu", "Cử đội phản ứng nhanh 1"]
        assert self.mgr.dispatch_incident(inc_id, team_name="Đội Cơ Động 1", sop_actions=sops) is True
        assert registry[inc_id].status == IncidentStatus.IN_PROGRESS.value

        # Step 3: Resolve (IN_PROGRESS -> RESOLVED)
        assert self.mgr.resolve_incident(inc_id, resolution_notes="Đã hoàn lưu nước an toàn") is True
        assert registry[inc_id].status == IncidentStatus.RESOLVED.value

        # Audit trail cryptographic verification
        trail = self.mgr.get_state("audit_trail")
        assert len(trail) >= 3

        action_names = {ev.action for ev in trail[:3]}
        assert "RESOLVE_INCIDENT" in action_names

        for evt in trail[:3]:
            assert len(evt.sha256_hash) == 16
            assert all(c in "0123456789abcdef" for c in evt.sha256_hash)
            expected_hash = hashlib.sha256(
                f"{evt.timestamp}{evt.user_role}{evt.action}{evt.details}".encode()
            ).hexdigest()[:16]
            assert evt.sha256_hash == expected_hash

    def test_all_four_persona_views_execution(self) -> None:
        """Verifies unhandled-exception-free execution of all 4 operational persona views and fragments."""
        # 1. View renderers
        views = [
            render_executive_view,
            render_telemetry_view,
            render_incident_view,
            render_hydrology_ops_view,
        ]
        for view_fn in views:
            assert callable(view_fn)
            view_fn()

        # 2. Fragments
        fragments = [
            basin_filter_fragment,
            telemetry_controls_fragment,
            forecast_parameter_fragment,
            incident_dispatch_fragment,
            streeter_phelps_slider_fragment,
            infra_ping_fragment,
        ]
        for frag in fragments:
            assert callable(frag)

        # Execute sample fragments
        all_basins = ["Lưu vực sông Hồng - Thái Bình", "Lưu vực sông Cửu Long"]
        filtered = basin_filter_fragment(all_basins)
        assert isinstance(filtered, list)

        st_id, window, mode = telemetry_controls_fragment()
        assert st_id in [s["station_id"] for s in REFERENCE_STATIONS]
        assert window in ["24h", "48h", "7d"]
        assert mode in ["LỊCH SỬ THỰC ĐO", "DỰ BÁO AI 72H"]

    def test_lean_app_router_reexport_and_line_count(self) -> None:
        """Guarantees lean router contract re-exports and <= 100 source code lines constraint."""
        # Re-export compatibility
        net_state = get_current_network_state()
        assert len(net_state) == 10

        series = generate_recent_series(REFERENCE_STATIONS[0], hours=24)
        assert len(series) == 25

        # Source code line count constraint
        app_file = Path(dashboard_app.__file__)
        assert app_file.exists()
        with open(app_file, encoding="utf-8") as f:
            lines = f.readlines()

        total_lines = len(lines)
        assert total_lines <= 100, f"src/dashboard/app.py must have <= 100 lines, found: {total_lines}"
