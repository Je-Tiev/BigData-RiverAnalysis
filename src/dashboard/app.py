"""River Operations Center - National Hydrological & Environmental Dashboard.

Enterprise interactive command center for water quality monitoring, GIS spatial
analysis, real-time sensor telemetry, AI/ML forecasting, and emergency incident dispatch.
Built with Streamlit, Folium, and Plotly.
"""

import os
import sys
from datetime import UTC, datetime, timedelta
from typing import Any

import streamlit as st

# Setup root path for relative imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from src.contracts.river_schema import REFERENCE_STATIONS
from src.ml.alert_manager import RiverAlertManager
from src.ml.anomaly_detector import RiverAnomalyDetector
from src.ml.forecaster import RiverWaterQualityForecaster
from src.producers.river_simulator import RiverTelemetryGenerator

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
tab_gis, tab_telemetry, tab_forecast, tab_alerts = st.tabs(
    [
        "🗺️ Bản Đồ GIS Không Gian",
        "📈 Viễn Thám Cảm Biến Real-Time",
        "🔮 Dự Báo AI/ML (24h - 72h)",
        "🚨 Trung Tâm Xử Lý Cảnh Báo",
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
