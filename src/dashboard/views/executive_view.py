"""Executive Overview Operational View (Screen 1: NOC-GIS Command Center)."""

import streamlit as st

from src.contracts.river_schema import REFERENCE_STATIONS
from src.dashboard.components.incident_card import render_incident_card
from src.dashboard.components.kpi_ribbon import render_kpi_ribbon
from src.dashboard.components.map_component import render_gis_network_map
from src.dashboard.services.telemetry_service import TelemetryService
from src.dashboard.state.session_state import IncidentStatus, SessionStateManager, safe_fragment


@safe_fragment
def basin_filter_fragment(all_basins: list[str]) -> list[str]:
    """Isolated fragment for basin filtering without triggering Folium map remount or flicker."""
    state_mgr = SessionStateManager()
    saved_basins = state_mgr.get_state("selected_basins", all_basins)
    default_selection = [b for b in saved_basins if b in all_basins] if saved_basins else all_basins

    col_filter, col_quick_sel = st.columns([3, 1])
    with col_filter:
        selected = st.multiselect(
            "Lọc Phân Vùng Lưu Vực Sông:",
            options=all_basins,
            default=default_selection,
            key="exec_basin_multiselect",
            help="Chọn các lưu vực để lọc trạm trên bản đồ và bảng tin sự cố.",
        )
    with col_quick_sel:
        st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
        if st.button("Chọn Tất Cả Lưu Vực", key="exec_select_all_basins_btn", use_container_width=True):
            selected = all_basins
            st.session_state["exec_basin_multiselect"] = all_basins

    state_mgr.set_state("selected_basins", selected)
    return selected


def render_executive_view() -> None:
    """Renders Screen 1: Top KPI Ribbon + 68% GIS Map + 32% Live Incident Feed."""
    state_mgr = SessionStateManager()
    telemetry_service = TelemetryService.get_instance()
    network_state = telemetry_service.get_current_network_state()

    # 1. Top SCADA Executive KPI Ribbon
    render_kpi_ribbon(network_state, streaming_latency_ms=820)
    st.markdown("<div style='height: 10px;'></div>", unsafe_allow_html=True)

    # 2. Isolated Basin Filter Fragment
    all_basins = sorted({st_meta.get("basin_name", "") for st_meta in REFERENCE_STATIONS if st_meta.get("basin_name")})
    selected_basins = basin_filter_fragment(all_basins)

    # 3. Split Layout: 68% Folium GIS Spatial Map vs 32% Live Incident Feed
    col_map, col_feed = st.columns([68, 32])

    with col_map:
        st.markdown(
            """
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; color: #06B6D4; letter-spacing: 0.05em;">
                    🗺️ BẢN ĐỒ GIÁM SÁT KHÔNG GIAN GIS (DARK MATTER)
                </div>
                <div style="font-size: 11px; color: #64748B;">CartoDB DarkMatter • 5-Tier WQI Markers</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        filtered_stations = (
            [s for s in network_state if s.get("basin_name") in selected_basins] if selected_basins else network_state
        )
        selected_station_id = state_mgr.get_state("selected_station_id", "ST_HONG_LB_01")

        # Map rendered outside fragments to eliminate iframe remounts and zoom resets
        render_gis_network_map(
            stations=filtered_stations,
            selected_station_id=selected_station_id,
            height=580,
            zoom_start=6,
        )

    with col_feed:
        st.markdown(
            """
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; color: #EF4444; letter-spacing: 0.05em;">
                    🚨 BẢNG TIN SỰ CỐ & CẢNH BÁO TRỰC TIẾP
                </div>
                <div style="font-size: 11px; color: #64748B;">Thời gian thực • Tác nghiệp SOP</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        all_incidents = state_mgr.get_incidents()
        open_incidents = [
            inc
            for inc in all_incidents
            if inc.get("status")
            in [IncidentStatus.NEW.value, IncidentStatus.ACKNOWLEDGED.value, IncidentStatus.IN_PROGRESS.value]
        ]

        if not open_incidents:
            st.success("Toàn bộ 10 trạm mạng lưới đều đạt chuẩn quy chuẩn an toàn QCVN 08:2023.")
        else:

            def _handle_ack(inc_id: str) -> None:
                state_mgr.acknowledge_incident(inc_id)
                st.toast(f"Đã tiếp nhận sự cố {inc_id}")
                st.rerun()

            def _handle_dispatch(inc_id: str) -> None:
                state_mgr.set_state("selected_incident_id", inc_id)
                st.toast(f"Đã chọn sự cố {inc_id}. Chuyển sang màn hình [INCIDENTS] để điều phối.")

            for inc in open_incidents:
                render_incident_card(
                    inc,
                    on_acknowledge=_handle_ack,
                    on_dispatch=_handle_dispatch,
                    key_prefix="exec_feed",
                )
