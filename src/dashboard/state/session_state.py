"""Centralized Session State Management, RBAC Configuration, and Cryptographic Audit Trail."""

import functools
import hashlib
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

import streamlit as st

from src.contracts.river_schema import REFERENCE_STATIONS


def safe_fragment(*f_args, **f_kwargs):
    """Streamlit fragment decorator that safely falls back to direct execution in bare mode / tests."""

    def decorator(fn):
        try:
            frag = st.fragment(*f_args, **f_kwargs)(fn)
        except Exception:  # noqa: BLE001
            frag = fn

        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                from streamlit.runtime.scriptrunner_utils import get_script_run_ctx

                ctx = get_script_run_ctx()
            except Exception:  # noqa: BLE001
                ctx = None

            if ctx is None:
                return fn(*args, **kwargs)
            return frag(*args, **kwargs)

        return wrapper

    if len(f_args) == 1 and callable(f_args[0]) and not f_kwargs:
        return decorator(f_args[0])
    return decorator


class UserRole(StrEnum):
    """4-Tier Persona Roles for SCADA NOC Operations."""

    EXECUTIVE = "EXECUTIVE"
    ENVIRONMENTAL_ENGINEER = "ENVIRONMENTAL_ENGINEER"
    FIELD_OFFICER = "FIELD_OFFICER"
    MLOPS_ENGINEER = "MLOPS_ENGINEER"


class IncidentStatus(StrEnum):
    """4-Stage SCADA Incident Lifecycle Statuses."""

    NEW = "NEW"  # Mới phát hiện từ cảm biến/mô hình
    ACKNOWLEDGED = "ACKNOWLEDGED"  # Ca trực đã tiếp nhận
    IN_PROGRESS = "IN_PROGRESS"  # Đội hiện trường đang xử lý
    RESOLVED = "RESOLVED"  # Đã khắc phục & bình thường hóa


class IncidentSeverity(StrEnum):
    """Standard SCADA Alert Severity Tiers."""

    CRITICAL = "CRITICAL"
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"


@dataclass(frozen=True)
class RoleProfile:
    """Profile definition and granular permissions for each operational persona."""

    role: UserRole
    display_title: str
    badge_label: str
    default_view: str
    allowed_screens: list[str]
    can_dispatch: bool
    can_export_reports: bool
    can_retrain_ml: bool
    can_trigger_dlq: bool


@dataclass
class AuditEvent:
    """Immutable cryptographic audit event record."""

    timestamp: str
    user_role: str
    action: str
    details: str
    sha256_hash: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "timestamp": self.timestamp,
            "user_role": self.user_role,
            "action": self.action,
            "details": self.details,
            "sha256_hash": self.sha256_hash,
        }


@dataclass
class IncidentRecord:
    """Operational incident entity in SCADA NOC registry."""

    incident_id: str
    station_id: str
    station_name: str
    river_name: str
    severity: str
    status: str
    timestamp: str
    anomaly_reason: str
    metrics_snapshot: dict[str, Any] = field(default_factory=dict)
    assigned_team: str | None = None
    sop_actions: list[str] = field(default_factory=list)
    resolution_notes: str | None = None
    sha256_seal: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "incident_id": self.incident_id,
            "station_id": self.station_id,
            "station_name": self.station_name,
            "river_name": self.river_name,
            "severity": self.severity,
            "status": self.status,
            "timestamp": self.timestamp,
            "anomaly_reason": self.anomaly_reason,
            "metrics_snapshot": self.metrics_snapshot,
            "assigned_team": self.assigned_team,
            "sop_actions": self.sop_actions,
            "resolution_notes": self.resolution_notes,
            "sha256_seal": self.sha256_seal,
        }


ROLE_REGISTRY: dict[UserRole, RoleProfile] = {
    UserRole.EXECUTIVE: RoleProfile(
        role=UserRole.EXECUTIVE,
        display_title="Giám Đốc Điều Hành Chiến Lược",
        badge_label="[ROLE: EXECUTIVE]",
        default_view="[NOC-GIS] Tổng Quan",
        allowed_screens=[
            "[NOC-GIS] Tổng Quan",
            "[TELEMETRY] Viễn Thám & Dự Báo",
            "[INCIDENTS] Tác Nghiệp Sự Cố",
        ],
        can_dispatch=False,
        can_export_reports=True,
        can_retrain_ml=False,
        can_trigger_dlq=False,
    ),
    UserRole.ENVIRONMENTAL_ENGINEER: RoleProfile(
        role=UserRole.ENVIRONMENTAL_ENGINEER,
        display_title="Kỹ Sư Môi Trường & Thủy Văn",
        badge_label="[ROLE: ENV_ENGINEER]",
        default_view="[TELEMETRY] Viễn Thám & Dự Báo",
        allowed_screens=[
            "[NOC-GIS] Tổng Quan",
            "[TELEMETRY] Viễn Thám & Dự Báo",
            "[HYDRO-OPS] Kỹ Thuật Thủy Văn",
        ],
        can_dispatch=False,
        can_export_reports=True,
        can_retrain_ml=False,
        can_trigger_dlq=False,
    ),
    UserRole.FIELD_OFFICER: RoleProfile(
        role=UserRole.FIELD_OFFICER,
        display_title="Cán Bộ Điều Phối Hiện Trường",
        badge_label="[ROLE: FIELD_OFFICER]",
        default_view="[INCIDENTS] Tác Nghiệp Sự Cố",
        allowed_screens=[
            "[NOC-GIS] Tổng Quan",
            "[INCIDENTS] Tác Nghiệp Sự Cố",
        ],
        can_dispatch=True,
        can_export_reports=True,
        can_retrain_ml=False,
        can_trigger_dlq=False,
    ),
    UserRole.MLOPS_ENGINEER: RoleProfile(
        role=UserRole.MLOPS_ENGINEER,
        display_title="Kỹ Sư Nền Tảng & MLOps",
        badge_label="[ROLE: MLOPS_ADMIN]",
        default_view="[HYDRO-OPS] Kỹ Thuật Thủy Văn",
        allowed_screens=[
            "[NOC-GIS] Tổng Quan",
            "[TELEMETRY] Viễn Thám & Dự Báo",
            "[INCIDENTS] Tác Nghiệp Sự Cố",
            "[HYDRO-OPS] Kỹ Thuật Thủy Văn",
        ],
        can_dispatch=True,
        can_export_reports=True,
        can_retrain_ml=True,
        can_trigger_dlq=True,
    ),
}


class SessionStateManager:
    """Singleton-friendly state orchestrator managing session variables, RBAC, and audit events."""

    _instance: "SessionStateManager | None" = None

    def __new__(cls) -> "SessionStateManager":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    @staticmethod
    def _build_initial_incidents() -> dict[str, IncidentRecord]:
        now_iso = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        inc1 = IncidentRecord(
            incident_id="INC-ST_DONGNAI_BH_05",
            station_id="ST_DONGNAI_BH_05",
            station_name="Trạm Cầu Hóa An",
            river_name="Sông Đồng Nai",
            severity="CRITICAL",
            status=IncidentStatus.NEW.value,
            timestamp=now_iso,
            anomaly_reason="Nồng độ oxy hòa tan (DO) giảm sâu dưới 3.2 mg/L và COD vượt ngưỡng 65.0 mg/L",
            metrics_snapshot={"dissolved_oxygen": 3.18, "cod": 68.5, "ammonia": 1.45},
            sop_actions=[],
            sha256_seal=hashlib.sha256(f"INC-ST_DONGNAI_BH_05_{now_iso}".encode()).hexdigest()[:16],
        )
        inc2 = IncidentRecord(
            incident_id="INC-ST_TIEN_MYTHO_08",
            station_id="ST_TIEN_MYTHO_08",
            station_name="Trạm TP Mỹ Tho",
            river_name="Sông Tiền",
            severity="HIGH",
            status=IncidentStatus.ACKNOWLEDGED.value,
            timestamp=now_iso,
            anomaly_reason="Độ mặn đột biến 5.2 ppt vượt ngưỡng 4.0 ppt do triều cường xâm nhập mặn",
            metrics_snapshot={"salinity": 5.2, "conductivity": 8200.0},
            sop_actions=["Đã thông báo nhà máy nước ngắt thu nước thô"],
            sha256_seal=hashlib.sha256(f"INC-ST_TIEN_MYTHO_08_{now_iso}".encode()).hexdigest()[:16],
        )
        inc3 = IncidentRecord(
            incident_id="INC-ST_SAIGON_SG_06",
            station_id="ST_SAIGON_SG_06",
            station_name="Trạm Bến Bạch Đằng",
            river_name="Sông Sài Gòn",
            severity="HIGH",
            status=IncidentStatus.IN_PROGRESS.value,
            timestamp=now_iso,
            anomaly_reason="Amoni tăng cao 2.1 mg/L và Coliform vượt 15000 MPN/100mL",
            metrics_snapshot={"ammonia": 2.1, "coliform": 18500},
            assigned_team="Đội 3 TP.HCM",
            sop_actions=["Đã bật cảnh báo trạm hạ lưu", "Đã lấy 3 mẫu đối chứng phòng thí nghiệm"],
            sha256_seal=hashlib.sha256(f"INC-ST_SAIGON_SG_06_{now_iso}".encode()).hexdigest()[:16],
        )
        inc4 = IncidentRecord(
            incident_id="INC-ST_HONG_LB_01",
            station_id="ST_HONG_LB_01",
            station_name="Trạm Cầu Long Biên",
            river_name="Sông Hồng",
            severity="MEDIUM",
            status=IncidentStatus.RESOLVED.value,
            timestamp=now_iso,
            anomaly_reason="Độ đục tăng do phù sa cát lòng sông thượng nguồn",
            metrics_snapshot={"turbidity_ntu": 450.0},
            assigned_team="Đội 1 Hà Nội",
            sop_actions=["Khảo sát hiện trường", "Đối chiếu cảm biến"],
            resolution_notes="Nước đã trong trở lại, các thông số về giới hạn an toàn.",
            sha256_seal=hashlib.sha256(f"INC-ST_HONG_LB_01_{now_iso}".encode()).hexdigest()[:16],
        )
        return {
            inc1.incident_id: inc1,
            inc2.incident_id: inc2,
            inc3.incident_id: inc3,
            inc4.incident_id: inc4,
        }

    def init_session_state(self, force: bool = False) -> None:
        """Initializes default variables in Streamlit session state if not already set."""
        all_basins = sorted({st_item.get("basin_name", "") for st_item in REFERENCE_STATIONS})

        if force or "user_role" not in st.session_state:
            st.session_state["user_role"] = UserRole.EXECUTIVE.value
        if force or "selected_station_id" not in st.session_state:
            st.session_state["selected_station_id"] = "ST_HONG_LB_01"
        if force or "selected_basins" not in st.session_state:
            st.session_state["selected_basins"] = all_basins
        if force or "telemetry_window" not in st.session_state:
            st.session_state["telemetry_window"] = "24h"
        if force or "telemetry_mode" not in st.session_state:
            st.session_state["telemetry_mode"] = "LỊCH SỬ THỰC ĐO"
        if force or "forecast_horizon" not in st.session_state:
            st.session_state["forecast_horizon"] = 72
        if force or "audit_trail" not in st.session_state:
            st.session_state["audit_trail"] = []
        if force or "incident_registry" not in st.session_state:
            st.session_state["incident_registry"] = self._build_initial_incidents()
        if "selected_incident_id" not in st.session_state:
            incidents = st.session_state["incident_registry"]
            if isinstance(incidents, dict) and incidents:
                st.session_state["selected_incident_id"] = next(iter(incidents.keys()))
            else:
                st.session_state["selected_incident_id"] = "INC-ST_DONGNAI_BH_05"

    def get_state(self, key: str, default: Any = None) -> Any:
        """Type-safe retrieval of state values."""
        if key in st.session_state:
            return st.session_state[key]
        return default

    def set_state(self, key: str, value: Any) -> None:
        """Type-safe assignment of state values."""
        st.session_state[key] = value

    def add_audit_event(
        self,
        action: str,
        details: str,
        user_role: str | None = None,
    ) -> AuditEvent:
        """Creates an immutable audit log record with 16-char SHA-256 cryptographic seal."""
        ts = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
        role = user_role or self.get_state("user_role", UserRole.EXECUTIVE.value)
        sha256_hash = hashlib.sha256(f"{ts}{role}{action}{details}".encode()).hexdigest()[:16]

        evt = AuditEvent(
            timestamp=ts,
            user_role=role,
            action=action,
            details=details,
            sha256_hash=sha256_hash,
        )
        trail = self.get_state("audit_trail", [])
        if not isinstance(trail, list):
            trail = []
        trail.insert(0, evt)
        self.set_state("audit_trail", trail)
        return evt

    def acknowledge_incident(self, incident_id: str, officer_name: str | None = None) -> bool:
        """Transitions incident status to ACKNOWLEDGED and logs audit record."""
        registry = self.get_state("incident_registry", {})
        if incident_id not in registry:
            return False

        inc = registry[incident_id]
        inc.status = IncidentStatus.ACKNOWLEDGED.value
        officer = officer_name or "Ca trực NOC"
        self.add_audit_event(
            action="ACKNOWLEDGE_INCIDENT",
            details=f"Cán bộ {officer} tiếp nhận xử lý sự cố {incident_id} tại trạm {inc.station_id}",
        )
        return True

    def dispatch_incident(
        self,
        incident_id: str,
        team_name: str,
        sop_actions: list[str],
        eta: str | None = None,
    ) -> bool:
        """Transitions incident to IN_PROGRESS, attaches SOPs & team, and logs audit record."""
        registry = self.get_state("incident_registry", {})
        if incident_id not in registry:
            return False

        inc = registry[incident_id]
        inc.status = IncidentStatus.IN_PROGRESS.value
        inc.assigned_team = team_name
        inc.sop_actions = sop_actions
        eta_str = f", ETA: {eta}" if eta else ""
        self.add_audit_event(
            action="DISPATCH_INCIDENT",
            details=f"Điều phối đội {team_name} xử lý sự cố {incident_id}{eta_str}. SOPs: {', '.join(sop_actions)}",
        )
        return True

    def resolve_incident(self, incident_id: str, resolution_notes: str) -> bool:
        """Transitions incident to RESOLVED and seals with final operational notes."""
        registry = self.get_state("incident_registry", {})
        if incident_id not in registry:
            return False

        inc = registry[incident_id]
        inc.status = IncidentStatus.RESOLVED.value
        inc.resolution_notes = resolution_notes
        self.add_audit_event(
            action="RESOLVE_INCIDENT",
            details=f"Giải quyết và đóng sự cố {incident_id}: {resolution_notes}",
        )
        return True

    def get_incidents(self, status: IncidentStatus | str | None = None) -> list[dict[str, Any]]:
        """Returns incidents as dictionaries filtered optionally by lifecycle status."""
        registry = self.get_state("incident_registry", {})
        status_val = status.value if isinstance(status, IncidentStatus) else (str(status).upper() if status else None)
        results = []
        items = registry.values() if isinstance(registry, dict) else registry
        for item in items:
            rec = item.to_dict() if hasattr(item, "to_dict") else item
            if status_val is None or rec.get("status", "").upper() == status_val:
                results.append(rec)
        return results

    def get_role_profile(self, role: UserRole | str | None = None) -> RoleProfile:
        """Retrieves permissions and profile for active or queried operational role."""
        role_val = role or self.get_state("user_role", UserRole.EXECUTIVE.value)
        if isinstance(role_val, str):
            try:
                role_enum = UserRole(role_val)
            except ValueError:
                role_enum = UserRole.EXECUTIVE
        else:
            role_enum = role_val
        return ROLE_REGISTRY[role_enum]

    def switch_role(self, new_role: UserRole | str) -> None:
        """Updates user persona and creates an audit entry."""
        role_val = new_role.value if isinstance(new_role, UserRole) else str(new_role)
        self.set_state("user_role", role_val)
        self.add_audit_event(
            action="ROLE_SWITCH",
            details=f"Chuyển đổi vai trò tác nghiệp sang: {role_val}",
            user_role=role_val,
        )


def add_audit_event(
    action: str,
    details: str,
    user_role: str | None = None,
) -> AuditEvent:
    """Module-level convenience wrapper logging an audit event into centralized SessionStateManager."""
    return SessionStateManager().add_audit_event(action=action, details=details, user_role=user_role)
