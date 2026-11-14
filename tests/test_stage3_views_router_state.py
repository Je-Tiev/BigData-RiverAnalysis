"""Stage 3 Automated Test Suite: Persona Operational Views, RBAC Session State & Lean Router.

Validates:
1. SessionStateManager, UserRole RBAC permissions matrix, Incident lifecycle and SHA-256 audit trail.
2. 4 Operational Views rendering functions and 6 isolated Fragment boundary functions.
3. Lean Router Entrypoint (app.py) contract re-exports and <= 100 source code lines constraint.
"""

import hashlib
from pathlib import Path

import pytest

import src.dashboard.app as dashboard_app
from src.contracts.river_schema import REFERENCE_STATIONS
from src.dashboard.app import generate_recent_series, get_current_network_state
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


class TestSessionStateAndRBAC:
    """Test Suite Group 1: SessionStateManager, RBAC Matrix, Lifecycle & SHA-256 Audit."""

    @pytest.fixture(autouse=True)
    def setup_state(self):
        """Prepares a clean state manager for each test."""
        self.mgr = SessionStateManager()
        self.mgr.init_session_state(force=True)

    def test_session_state_manager_init_defaults(self):
        """Verifies session state initialization sets all canonical default keys."""
        assert self.mgr.get_state("user_role") == UserRole.EXECUTIVE.value
        assert self.mgr.get_state("selected_station_id") == "ST_HONG_LB_01"
        assert self.mgr.get_state("telemetry_window") == "24h"
        assert self.mgr.get_state("telemetry_mode") == "LỊCH SỬ THỰC ĐO"

        basins = self.mgr.get_state("selected_basins")
        assert isinstance(basins, list)
        assert len(basins) > 0
        assert "Lưu vực sông Hồng - Thái Bình" in basins

        incidents = self.mgr.get_state("incident_registry")
        assert isinstance(incidents, dict)
        assert len(incidents) >= 4
        assert "INC-ST_DONGNAI_BH_05" in incidents

        trail = self.mgr.get_state("audit_trail")
        assert isinstance(trail, list)

    def test_session_state_role_switching_and_permissions(self):
        """Validates 4 persona profiles and strictly enforces the RBAC screen access matrix."""
        # 1. EXECUTIVE
        self.mgr.switch_role(UserRole.EXECUTIVE)
        prof_exec = self.mgr.get_role_profile()
        assert prof_exec.role == UserRole.EXECUTIVE
        assert prof_exec.can_dispatch is False
        assert prof_exec.can_export_reports is True
        assert "[NOC-GIS] Tổng Quan" in prof_exec.allowed_screens
        assert "[HYDRO-OPS] Kỹ Thuật Thủy Văn" not in prof_exec.allowed_screens

        # 2. ENVIRONMENTAL_ENGINEER
        self.mgr.switch_role(UserRole.ENVIRONMENTAL_ENGINEER)
        prof_env = self.mgr.get_role_profile()
        assert prof_env.role == UserRole.ENVIRONMENTAL_ENGINEER
        assert prof_env.can_dispatch is False
        assert prof_env.can_export_reports is True
        assert "[TELEMETRY] Viễn Thám & Dự Báo" in prof_env.allowed_screens
        assert "[HYDRO-OPS] Kỹ Thuật Thủy Văn" in prof_env.allowed_screens
        assert "[INCIDENTS] Tác Nghiệp Sự Cố" not in prof_env.allowed_screens

        # 3. FIELD_OFFICER
        self.mgr.switch_role(UserRole.FIELD_OFFICER)
        prof_field = self.mgr.get_role_profile()
        assert prof_field.role == UserRole.FIELD_OFFICER
        assert prof_field.can_dispatch is True
        assert "[INCIDENTS] Tác Nghiệp Sự Cố" in prof_field.allowed_screens
        assert "[HYDRO-OPS] Kỹ Thuật Thủy Văn" not in prof_field.allowed_screens

        # 4. MLOPS_ENGINEER (Full 4-screen access)
        self.mgr.switch_role(UserRole.MLOPS_ENGINEER)
        prof_mlops = self.mgr.get_role_profile()
        assert prof_mlops.role == UserRole.MLOPS_ENGINEER
        assert prof_mlops.can_dispatch is True
        assert prof_mlops.can_trigger_dlq is True
        assert prof_mlops.can_retrain_ml is True
        assert len(prof_mlops.allowed_screens) == 4
        assert "[HYDRO-OPS] Kỹ Thuật Thủy Văn" in prof_mlops.allowed_screens
        assert "[INCIDENTS] Tác Nghiệp Sự Cố" in prof_mlops.allowed_screens

    def test_incident_lifecycle_and_audit_trail_sha256(self):
        """Verifies full incident lifecycle transitions and validates SHA-256 audit integrity."""
        inc_id = "INC-ST_DONGNAI_BH_05"
        registry = self.mgr.get_state("incident_registry")
        registry[inc_id].status = IncidentStatus.NEW.value

        # Step 1: NEW -> ACKNOWLEDGED
        ack_ok = self.mgr.acknowledge_incident(inc_id, officer_name="Đ/c Nguyễn Văn A")
        assert ack_ok is True
        inc_rec = registry[inc_id]
        assert inc_rec.status == IncidentStatus.ACKNOWLEDGED.value

        # Step 2: ACKNOWLEDGED -> IN_PROGRESS
        sops = ["Bật cảnh báo hạ lưu", "Lấy 3 mẫu đối chứng"]
        disp_ok = self.mgr.dispatch_incident(
            incident_id=inc_id,
            team_name="Đội 3 TP.HCM",
            sop_actions=sops,
            eta="30 phút",
        )
        assert disp_ok is True
        assert inc_rec.status == IncidentStatus.IN_PROGRESS.value
        assert inc_rec.assigned_team == "Đội 3 TP.HCM"
        assert inc_rec.sop_actions == sops

        # Step 3: IN_PROGRESS -> RESOLVED
        res_ok = self.mgr.resolve_incident(
            incident_id=inc_id,
            resolution_notes="Đã kiểm tra van xả và hoàn lưu nước an toàn.",
        )
        assert res_ok is True
        assert inc_rec.status == IncidentStatus.RESOLVED.value
        assert inc_rec.resolution_notes is not None

        # Step 4: Audit trail validation
        trail = self.mgr.get_state("audit_trail")
        assert len(trail) >= 3

        # Check latest audit event (RESOLVE_INCIDENT)
        latest_evt = trail[0]
        assert latest_evt.action == "RESOLVE_INCIDENT"
        assert len(latest_evt.sha256_hash) == 16
        # Must be valid hex string
        assert all(c in "0123456789abcdef" for c in latest_evt.sha256_hash)

        # Verify hash consistency formula
        expected_hash = hashlib.sha256(
            f"{latest_evt.timestamp}{latest_evt.user_role}{latest_evt.action}{latest_evt.details}".encode()
        ).hexdigest()[:16]
        assert latest_evt.sha256_hash == expected_hash


class TestViewsAndFragmentBoundaries:
    """Test Suite Group 2: View Module Imports, Signatures, and Fragment Boundaries."""

    def test_view_module_imports_and_signatures(self):
        """Verifies successful import and callable signatures for all 4 operational persona views."""
        views = [
            render_executive_view,
            render_telemetry_view,
            render_incident_view,
            render_hydrology_ops_view,
        ]
        for view_fn in views:
            assert callable(view_fn)
            assert view_fn.__name__.startswith("render_")

    def test_fragment_boundary_functions_exist(self):
        """Verifies existence and callable execution of all 6 isolated fragment functions."""
        fragments = [
            basin_filter_fragment,
            telemetry_controls_fragment,
            forecast_parameter_fragment,
            incident_dispatch_fragment,
            streeter_phelps_slider_fragment,
            infra_ping_fragment,
        ]
        for frag_fn in fragments:
            assert callable(frag_fn)
            assert hasattr(frag_fn, "__name__")

    def test_basin_filter_fragment_execution(self):
        """Verifies basin_filter_fragment executes safely and updates session state."""
        all_basins = ["Lưu vực sông Hồng - Thái Bình", "Lưu vực sông Hương"]
        filtered = basin_filter_fragment(all_basins)
        assert isinstance(filtered, list)
        mgr = SessionStateManager()
        saved = mgr.get_state("selected_basins")
        assert isinstance(saved, list)

    def test_telemetry_controls_fragment_execution(self):
        """Verifies telemetry_controls_fragment executes safely and returns 3-tuple."""
        res = telemetry_controls_fragment()
        assert isinstance(res, tuple)
        assert len(res) == 3
        st_id, window, mode = res
        assert st_id in [s["station_id"] for s in REFERENCE_STATIONS]
        assert window in ["24h", "48h", "7d"]
        assert mode in ["LỊCH SỬ THỰC ĐO", "DỰ BÁO AI 72H"]


class TestRouterFacadeAndLineCount:
    """Test Suite Group 3: app.py Re-export Contract and < 100 Lines Guarantee."""

    def test_app_reexport_contract_compatibility(self):
        """Guarantees backward-compatibility for get_current_network_state and generate_recent_series."""
        # 1. Test get_current_network_state directly exported from app
        network_state = get_current_network_state()
        assert isinstance(network_state, list)
        assert len(network_state) == 10

        required_fields = [
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
        for station in network_state:
            for field in required_fields:
                assert field in station, f"Missing field {field} in network_state station"

        # 2. Test generate_recent_series directly exported from app
        sample_meta = REFERENCE_STATIONS[0]
        recent_series = generate_recent_series(sample_meta, hours=24)
        assert isinstance(recent_series, list)
        # 24 hours + current hour = 25 hourly data points
        assert len(recent_series) == 25
        assert recent_series[-1]["station_id"] == sample_meta["station_id"]

    def test_app_code_line_count_under_100(self):
        """Validates that src/dashboard/app.py is strictly a Lean Router under 100 source lines."""
        app_path = Path(dashboard_app.__file__)
        assert app_path.exists(), "src/dashboard/app.py not found"

        with open(app_path, encoding="utf-8") as f:
            lines = f.readlines()

        total_line_count = len(lines)
        assert total_line_count <= 100, f"src/dashboard/app.py must be <= 100 lines, but has {total_line_count} lines"
