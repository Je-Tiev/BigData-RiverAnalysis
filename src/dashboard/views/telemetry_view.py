"""Telemetry Time Series and AI Multi-Step Forecasting Operational View (Screen 2: TELEMETRY)."""

from typing import Any

import streamlit as st

from src.contracts.river_schema import REFERENCE_STATIONS
from src.dashboard.components.charts_component import render_telemetry_grid
from src.dashboard.components.forecast_component import render_forecast_chart
from src.dashboard.services.api_client import FastAPIClient
from src.dashboard.services.export_service import ReportExportService
from src.dashboard.services.telemetry_service import TelemetryService
from src.dashboard.state.session_state import SessionStateManager, safe_fragment
from src.dashboard.styles.colors import get_wqi_color, get_wqi_tier_label


@safe_fragment
def telemetry_controls_fragment() -> tuple[str, str, str]:
    """Isolated fragment controlling station selection, time horizon, and viewing mode."""
    state_mgr = SessionStateManager()

    current_station_id = state_mgr.get_state("selected_station_id", "ST_HONG_LB_01")
    current_window = state_mgr.get_state("telemetry_window", "24h")
    current_mode = state_mgr.get_state("telemetry_mode", "LỊCH SỬ THỰC ĐO")

    station_ids = [s["station_id"] for s in REFERENCE_STATIONS]
    st_idx = station_ids.index(current_station_id) if current_station_id in station_ids else 0

    c1, c2, c3 = st.columns([2, 1, 1.2])

    with c1:
        selected_station_id = st.selectbox(
            "Chọn Trạm Quan Trắc SCADA:",
            options=station_ids,
            index=st_idx,
            format_func=lambda sid: next(
                (
                    f"{s['station_name']} ({sid}) - {s['river_name']}"
                    for s in REFERENCE_STATIONS
                    if s["station_id"] == sid
                ),
                sid,
            ),
            key="telemetry_station_selector",
        )

    with c2:
        window_options = ["24h", "48h", "7d"]
        w_idx = window_options.index(current_window) if current_window in window_options else 0
        selected_window = st.selectbox(
            "Khung Thời Gian:",
            options=window_options,
            index=w_idx,
            key="telemetry_window_selector",
        )

    with c3:
        mode_options = ["LỊCH SỬ THỰC ĐO", "DỰ BÁO AI 72H"]
        m_idx = mode_options.index(current_mode) if current_mode in mode_options else 0
        selected_mode = st.radio(
            "Chế Độ Hiển Thị:",
            options=mode_options,
            index=m_idx,
            horizontal=True,
            key="telemetry_mode_selector",
        )

    state_mgr.set_state("selected_station_id", selected_station_id)
    state_mgr.set_state("telemetry_window", selected_window)
    state_mgr.set_state("telemetry_mode", selected_mode)

    return selected_station_id, selected_window, selected_mode


@safe_fragment
def forecast_parameter_fragment(station_meta: dict[str, Any], hist_data: list[dict[str, Any]]) -> None:
    """Isolated fragment managing AI forecast execution, horizon tuning, and conformal ribbon rendering."""
    col_param, col_horizon = st.columns([2, 2])

    with col_param:
        target_param = st.selectbox(
            "Thông Số Dự Báo Quỹ Đạo AI:",
            options=["dissolved_oxygen", "ph", "ammonia", "cod"],
            format_func=lambda p: {
                "dissolved_oxygen": "Oxy hòa tan (DO - mg/L)",
                "ph": "Độ kiềm/axit (pH)",
                "ammonia": "Amoni tổng số (NH4-N - mg/L)",
                "cod": "Nhu cầu oxy hóa học (COD - mg/L)",
            }.get(p, p),
            key="forecast_target_param_selector",
        )

    with col_horizon:
        horizon_hours = st.slider(
            "Độ Dài Tầm Dự Báo (Giờ):",
            min_value=24,
            max_value=72,
            value=72,
            step=12,
            key="forecast_horizon_slider",
        )

    with st.spinner("Đang tính toán quỹ đạo dự báo AI qua LSTM / Temporal Conformal Predictor..."):
        api_client = FastAPIClient()
        current_reading = hist_data[-1] if hist_data else {}
        payload = {
            "station_id": station_meta.get("station_id", "ST_UNKNOWN"),
            "river_name": station_meta.get("river_name", "Unknown River"),
            "current_reading": current_reading,
            "recent_history": hist_data,
            "horizon_hours": horizon_hours,
        }
        forecast_result = api_client.predict_forecast(payload)

    render_forecast_chart(forecast_result, target_param=target_param, height=380)


def render_telemetry_view() -> None:
    """Renders Screen 2: Station Telemetry, 18-Param Plotly Grid, 72h Forecast & Multi-format Export."""
    state_mgr = SessionStateManager()
    telemetry_service = TelemetryService.get_instance()

    # 1. Isolated Filter & Control Fragment
    station_id, window_str, mode = telemetry_controls_fragment()

    # Find station metadata
    station_meta = next((s for s in REFERENCE_STATIONS if s["station_id"] == station_id), REFERENCE_STATIONS[0])
    hours_map = {"24h": 24, "48h": 48, "7d": 168}
    hours = hours_map.get(window_str, 24)

    # 2. Fetch Time Series Records
    hist_data = telemetry_service.generate_recent_series(station_meta, hours=hours)
    latest_reading = hist_data[-1] if hist_data else {}

    # 3. Station Technical Header Card
    wqi_val = float(latest_reading.get("vn_wqi_score") or latest_reading.get("ccme_score", 85.0))
    wqi_color = get_wqi_color(wqi_val)
    wqi_label = get_wqi_tier_label(wqi_val)

    st.markdown(
        f"""
        <div class="scada-metric-tile" style="margin-top: 10px; margin-bottom: 16px; border-left: 4px solid {wqi_color};">
            <div style="display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap;">
                <div>
                    <span style="font-family: 'JetBrains Mono', monospace; font-size: 16px; font-weight: 700; color: #F8FAFC;">
                        {station_meta.get("station_name")}
                    </span>
                    <span style="font-family: 'JetBrains Mono', monospace; font-size: 12px; color: #06B6D4; margin-left: 8px;">
                        [{station_meta.get("station_id")}]
                    </span>
                    <div style="font-size: 12px; color: #94A3B8; margin-top: 4px;">
                        Lưu vực: <strong>{station_meta.get("river_name")}</strong> ({station_meta.get("basin_name")}) •
                        Tọa độ: <strong>{float(station_meta.get("latitude", 0)):.4f}°N, {float(station_meta.get("longitude", 0)):.4f}°E</strong> •
                        Cao độ: <strong>{station_meta.get("elevation_m", 0)}m</strong>
                    </div>
                </div>
                <div style="text-align: right;">
                    <div style="font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #94A3B8;">CHỈ SỐ CHẤT LƯỢNG NƯỚC (WQI)</div>
                    <div style="font-family: 'JetBrains Mono', monospace; font-size: 24px; font-weight: 800; color: {wqi_color};">
                        {wqi_val:.1f} <span style="font-size: 13px; font-weight: 600;">[{wqi_label.upper()}]</span>
                    </div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    # 4. Content Division: Historical Grid vs AI Forecast Trajectory
    if mode == "DỰ BÁO AI 72H":
        st.markdown(
            """
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; color: #06B6D4; letter-spacing: 0.05em; margin-bottom: 8px;">
                📈 DỰ BÁO QUỸ ĐẠO CHẤT LƯỢNG NƯỚC 72H KÈM DẢI TIN CẬY 95% CI (CONFORMAL PREDICTION)
            </div>
            """,
            unsafe_allow_html=True,
        )
        forecast_parameter_fragment(station_meta, hist_data)
    else:
        st.markdown(
            """
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; color: #06B6D4; letter-spacing: 0.05em; margin-bottom: 8px;">
                📊 LƯỚI VIỄN THÁM 18 THÔNG SỐ CẢM BIẾN SCADA & ĐƯỜNG GIỚI HẠN AN TOÀN (QCVN 08:2023)
            </div>
            """,
            unsafe_allow_html=True,
        )
        render_telemetry_grid(hist_data, height=270)

    # 5. Technical Report Export Toolbar
    st.markdown("<div style='height: 18px;'></div>", unsafe_allow_html=True)
    st.markdown(
        """
        <div style="font-family: 'JetBrains Mono', monospace; font-size: 12px; font-weight: 700; color: #94A3B8; text-transform: uppercase; margin-bottom: 8px;">
            🖨️ THANH CÔNG CỤ XUẤT BÁO CÁO KỸ THUẬT QUAN TRẮC
        </div>
        """,
        unsafe_allow_html=True,
    )

    exp_col1, exp_col2, exp_col3 = st.columns(3)

    # 1. CSV Download Button
    with exp_col1:
        csv_bytes = ReportExportService.export_to_csv(hist_data)
        st.download_button(
            label="📥 Xuất Báo Cáo CSV (UTF-8)",
            data=csv_bytes,
            file_name=f"SCADA_Telemetry_{station_id}_{window_str}.csv",
            mime="text/csv",
            use_container_width=True,
            key=f"dl_csv_{station_id}",
        )

    # 2. Excel 4-Sheet Multi-Tab Download Button
    with exp_col2:
        audit_records = [
            evt.to_dict() if hasattr(evt, "to_dict") else evt for evt in state_mgr.get_state("audit_trail", [])
        ]
        xlsx_bytes = ReportExportService.export_to_excel(
            station_meta=station_meta,
            telemetry_data=hist_data,
            forecast_data=None,
            audit_data=audit_records,
        )
        st.download_button(
            label="📊 Xuất Excel Đa Trang (4 Sheets)",
            data=xlsx_bytes,
            file_name=f"SCADA_Audit_Report_{station_id}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True,
            key=f"dl_xlsx_{station_id}",
        )

    # 3. HTML Print-Ready Official A4 Download Button
    with exp_col3:
        wqi_summary = {
            "vn_wqi": wqi_val,
            "vn_label": wqi_label,
            "ccme": float(latest_reading.get("ccme_score", wqi_val)),
        }
        html_str = ReportExportService.export_to_html_report(
            station_meta=station_meta,
            telemetry_data=hist_data,
            wqi_summary=wqi_summary,
        )
        st.download_button(
            label="🖨️ In Báo Cáo A4 Chuẩn Nhà Nước (HTML)",
            data=html_str,
            file_name=f"Official_Water_Report_{station_id}.html",
            mime="text/html",
            use_container_width=True,
            key=f"dl_html_{station_id}",
        )
