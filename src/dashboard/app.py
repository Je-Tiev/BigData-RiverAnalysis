"""National River Hydrological SCADA Operations Center - Lean Router Entrypoint."""

import os
import sys
from typing import Any

import streamlit as st

# Setup root path for relative imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from src.dashboard.services.telemetry_service import TelemetryService
from src.dashboard.state.session_state import ROLE_REGISTRY, SessionStateManager, UserRole
from src.dashboard.styles.theme import apply_noc_theme
from src.dashboard.views.executive_view import render_executive_view
from src.dashboard.views.hydrology_ops_view import render_hydrology_ops_view
from src.dashboard.views.incident_view import render_incident_view
from src.dashboard.views.telemetry_view import render_telemetry_view

# 1. Page Configuration & Industrial NOC Theme Injection
st.set_page_config(
    layout="wide",
    page_title="Hệ Thống SCADA Thủy Văn Quốc Gia | National River Operations Center",
    page_icon="🌊",
    initial_sidebar_state="expanded",
)
apply_noc_theme()

# 2. Centralized Session State & RBAC Profile Initialization
state_mgr = SessionStateManager()
state_mgr.init_session_state()

# 3. Backward-Compatibility Contract Re-exports for Automated Tests
_telemetry_service = TelemetryService.get_instance()


def get_current_network_state() -> list[dict[str, Any]]:
    """Contract Re-export for tests/test_dashboard_and_observability.py."""
    return _telemetry_service.get_current_network_state()


def generate_recent_series(station_meta: dict[str, Any], hours: int = 24) -> list[dict[str, Any]]:
    """Contract Re-export for tests/test_dashboard_and_observability.py."""
    return _telemetry_service.generate_recent_series(station_meta, hours=hours)


# 4. Sidebar Branding & Persona Role Switcher
st.sidebar.markdown(
    """<div style="font-family: 'JetBrains Mono', monospace; font-size: 14px; font-weight: 700; color: #06B6D4; letter-spacing: 0.05em; padding-bottom: 4px;">HỆ THỐNG SCADA THỦY VĂN</div>
    <div style="font-size: 11px; color: #94A3B8; margin-bottom: 12px;">Trung Tâm Điều Hành Quốc Gia NOC v2.0</div>""",
    unsafe_allow_html=True,
)

current_role_val = state_mgr.get_state("user_role", UserRole.EXECUTIVE.value)
role_options = [r.value for r in UserRole]
selected_role_val = st.sidebar.selectbox(
    "Vai Trò Vận Hành (RBAC Persona):",
    options=role_options,
    index=role_options.index(current_role_val) if current_role_val in role_options else 0,
    format_func=lambda r: f"{ROLE_REGISTRY[UserRole(r)].badge_label} {ROLE_REGISTRY[UserRole(r)].display_title}",
    key="persona_role_selector",
)
if selected_role_val != current_role_val:
    state_mgr.switch_role(selected_role_val)

# 5. Role Profile & View Permissions Navigation
profile = state_mgr.get_role_profile()
allowed_screens = profile.allowed_screens

st.sidebar.markdown("---")
st.sidebar.markdown(
    """<div style="font-size: 10px; font-weight: 700; color: #64748B; font-family: 'JetBrains Mono', monospace; text-transform: uppercase; margin-bottom: 6px;">DANH MỤC MÀN HÌNH TÁC NGHIỆP</div>""",
    unsafe_allow_html=True,
)
active_view = st.sidebar.radio(
    "Điều hướng màn hình:",
    options=allowed_screens,
    index=0,
    label_visibility="collapsed",
    key="nav_view_selector",
)

st.sidebar.markdown("---")
st.sidebar.caption("Trạng thái Ingest: 1.2k evt/s | Kafka KRaft | Schema: v1.3.0")

# 6. View Dispatcher Execution
VIEW_DISPATCHER = {
    "[NOC-GIS] Tổng Quan": render_executive_view,
    "[TELEMETRY] Viễn Thám & Dự Báo": render_telemetry_view,
    "[INCIDENTS] Tác Nghiệp Sự Cố": render_incident_view,
    "[HYDRO-OPS] Kỹ Thuật Thủy Văn": render_hydrology_ops_view,
}
render_func = VIEW_DISPATCHER.get(active_view, render_executive_view)
render_func()
