"""Advanced Hydrological Engineering, MLOps Diagnostics & Infrastructure Probes (Screen 4: HYDRO-OPS)."""

import json

import numpy as np
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.components.infra_status_badge import render_infra_status_badges
from src.dashboard.state.session_state import safe_fragment
from src.dashboard.styles.theme import get_plotly_noc_layout, render_scada_metric_tile
from src.hydrology.propagation_engine import StreeterPhelpsSagModel
from src.ml.drift_and_conformal import HydrologicalDriftMonitor
from src.quality.dlq_recovery import DLQRecoveryEngine
from src.quality.ecological_risk import EmersonAmmoniaKinetics, QCVN082023Classifier
from src.quality.sensor_diagnostics import AdaptiveKalmanFilter1D, SensorFaultDetector


@safe_fragment(run_every="10s")
def infra_ping_fragment() -> None:
    """Background probe polling 6 infrastructure services every 10s."""
    render_infra_status_badges(timeout_sec=0.8)


@safe_fragment
def streeter_phelps_slider_fragment() -> None:
    """Isolated fragment managing Streeter-Phelps sag curve calculations and Plotly rendering."""
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        bod0 = st.slider("BOD Ban Đầu (L₀ - mg/L):", min_value=2.0, max_value=50.0, value=20.0, step=1.0)
    with c2:
        do0 = st.slider("DO Ban Đầu (C₀ - mg/L):", min_value=3.0, max_value=10.0, value=7.5, step=0.5)
    with c3:
        temp = st.slider("Nhiệt Độ Nước (°C):", min_value=15.0, max_value=40.0, value=28.0, step=0.5)
    with c4:
        velocity = st.slider("Vận Tốc Dòng (U - m/s):", min_value=0.2, max_value=3.0, value=0.8, step=0.1)

    model = StreeterPhelpsSagModel()
    res = model.simulate_sag_curve(
        initial_bod=bod0,
        initial_do=do0,
        temperature_c=temp,
        flow_velocity_mps=velocity,
        max_travel_hours=72.0,
        step_hours=1.0,
    )

    # 4 Hydrological Indicators
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.markdown(
            render_scada_metric_tile("DO BÃO HÒA (Cs)", f"{res.do_saturation_mg_l:.2f}", "mg/L", "#06B6D4"),
            unsafe_allow_html=True,
        )
    with m2:
        st.markdown(
            render_scada_metric_tile(
                "ĐÁY VÕNG DO_MIN",
                f"{res.minimum_do_mg_l:.2f}",
                "mg/L",
                "#EF4444" if res.minimum_do_mg_l < 4.0 else "#10B981",
            ),
            unsafe_allow_html=True,
        )
    with m3:
        st.markdown(
            render_scada_metric_tile("THỜI GIAN VÕNG (tc)", f"{res.critical_travel_time_hours:.1f}", "GIỜ", "#F59E0B"),
            unsafe_allow_html=True,
        )
    with m4:
        st.markdown(
            render_scada_metric_tile("CỰ LY ĐÁY VÕNG (xc)", f"{res.critical_distance_km:.1f}", "KM", "#8B5CF6"),
            unsafe_allow_html=True,
        )

    # Plotly Sag Curve
    hours_arr = [p.travel_time_hours for p in res.trajectory]
    do_arr = [p.dissolved_oxygen_mg_l for p in res.trajectory]
    bod_arr = [p.bod_remaining_mg_l for p in res.trajectory]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=hours_arr,
            y=do_arr,
            mode="lines",
            name="Oxy Hòa Tan (DO mg/L)",
            line={"color": "#06B6D4", "width": 3},
            hovertemplate="Thời gian: %{x:.1f}h<br>DO: <b>%{y:.2f} mg/L</b><extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=hours_arr,
            y=bod_arr,
            mode="lines",
            name="Suy Giảm BOD (mg/L)",
            line={"color": "#64748B", "width": 1.5, "dash": "dot"},
            hovertemplate="Thời gian: %{x:.1f}h<br>BOD: <b>%{y:.2f} mg/L</b><extra></extra>",
        )
    )

    # Minimum Sag Point Marker
    fig.add_trace(
        go.Scatter(
            x=[res.critical_travel_time_hours],
            y=[res.minimum_do_mg_l],
            mode="markers+text",
            name="Đáy Võng Cực Tiểu (DO_min)",
            marker={"size": 12, "color": "#EF4444", "symbol": "diamond"},
            text=[f"DO_min: {res.minimum_do_mg_l:.2f} mg/L"],
            textposition="bottom center",
            textfont={"color": "#EF4444", "size": 11, "family": "JetBrains Mono, monospace"},
        )
    )

    # Ecological Hypoxia Threshold 4.0 mg/L
    fig.add_hline(
        y=4.0,
        line_dash="dash",
        line_color="#EF4444",
        line_width=1.5,
        annotation_text="Ngưỡng Ngạt Thủy Sinh (4.0 mg/L)",
        annotation_position="top right",
        annotation_font={"size": 10, "color": "#EF4444", "family": "JetBrains Mono, monospace"},
    )

    layout = get_plotly_noc_layout(
        title="ĐƯỜNG CONG VÕNG OXY THEO THỜI GIAN TRÔI DÒNG (STREETER-PHELPS SAG CURVE)", height=380
    )
    fig.update_layout(layout)
    fig.update_xaxes(title="Thời Gian Trôi Dạt Dòng Chảy (Giờ)")
    fig.update_yaxes(title="Nồng Độ (mg/L)")
    st.plotly_chart(fig, use_container_width=True)


def render_hydrology_ops_view() -> None:
    """Renders Screen 4: Streeter-Phelps Sag, Emerson Kinetics, Kalman Filter, MLOps Drift & DLQ Console."""
    # 1. Periodic Infrastructure Health Probes
    st.markdown(
        """
        <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; color: #06B6D4; letter-spacing: 0.05em;">
                🌐 GIÁM SÁT SỨC KHỎE HẠ TẦNG VÀ ĐIỀU KHIỂN CỔNG KẾT NỐI (PING 10S)
            </div>
            <div style="font-size: 11px; color: #64748B;">Tự động thăm dò socket / HTTP • Chống tắc nghẽn giao diện</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    infra_ping_fragment()

    st.markdown("<div style='height: 14px;'></div>", unsafe_allow_html=True)

    # 2. 4 Technical Tabs
    tab1, tab2, tab3, tab4 = st.tabs(
        [
            "📉 1. Võng Oxy Streeter-Phelps",
            "🧪 2. Động Học Emerson NH3 & QCVN 08",
            "📡 3. Chẩn Đoán Cảm Biến & Kalman 1D",
            "🧠 4. MLOps Drift & Bàn Cứu DLQ",
        ]
    )

    # Tab 1: Streeter-Phelps Sag Curve Simulation
    with tab1:
        st.markdown(
            """
            <div style="font-size: 12px; color: #94A3B8; margin-bottom: 12px;">
                Mô phỏng cơ chế tự làm sạch tự nhiên của dòng sông theo định luật phân hủy sinh hóa BOD (k₁) và tái cấp khí bề mặt (k₂).
            </div>
            """,
            unsafe_allow_html=True,
        )
        streeter_phelps_slider_fragment()

    # Tab 2: Emerson Ammonia Kinetics & QCVN 08:2023 2D Heatmap
    with tab2:
        st.markdown(
            """
            <div style="font-size: 12px; color: #94A3B8; margin-bottom: 12px;">
                Tính toán nồng độ amoni tự do độc hại (NH₃) không ion hóa theo mô hình động học Emerson et al. (1975) và phân hạng chất lượng nước QCVN 08:2023/BTNMT.
            </div>
            """,
            unsafe_allow_html=True,
        )
        col_em1, col_em2, col_em3 = st.columns(3)
        with col_em1:
            ammonia_in = st.slider("Amoni Tổng Số (NH₄-N mg/L):", min_value=0.1, max_value=20.0, value=2.5, step=0.1)
        with col_em2:
            ph_in = st.slider("Độ pH Mẫu Nước:", min_value=6.0, max_value=9.5, value=8.2, step=0.1)
        with col_em3:
            temp_in = st.slider("Nhiệt Độ Nước Thực Đo (°C):", min_value=15.0, max_value=40.0, value=29.0, step=0.5)

        free_frac = EmersonAmmoniaKinetics.compute_free_fraction(ph=ph_in, temperature_c=temp_in)
        free_nh3 = EmersonAmmoniaKinetics.compute_free_ammonia(
            ammonia_total=ammonia_in, ph=ph_in, temperature_c=temp_in
        )
        toxicity = EmersonAmmoniaKinetics.evaluate_toxicity_level(free_nh3)

        classifier = QCVN082023Classifier()
        sample_dict = {
            "dissolved_oxygen": 6.0,
            "ph": ph_in,
            "ammonia": ammonia_in,
            "cod": 20.0,
            "tss": 30.0,
        }
        cls_result = classifier.classify_reading(sample_dict)
        overall_tier = cls_result.get("overall_tier", "B")

        t1, t2, t3, t4 = st.columns(4)
        with t1:
            st.markdown(
                render_scada_metric_tile("TỶ LỆ NH3 TỰ DO (f)", f"{free_frac * 100:.2f}", "%", "#06B6D4"),
                unsafe_allow_html=True,
            )
        with t2:
            st.markdown(
                render_scada_metric_tile(
                    "NỒNG ĐỘ NH3 TỰ DO", f"{free_nh3:.4f}", "mg/L", "#EF4444" if free_nh3 > 0.05 else "#10B981"
                ),
                unsafe_allow_html=True,
            )
        with t3:
            st.markdown(
                render_scada_metric_tile(
                    "ĐỘ ĐỘC SINH HỌC",
                    toxicity,
                    "CẤP ĐỘ",
                    "#EF4444" if "NGUY CƠ" in toxicity or "CỰC KỲ" in toxicity else "#10B981",
                ),
                unsafe_allow_html=True,
            )
        with t4:
            st.markdown(
                render_scada_metric_tile("PHÂN HẠNG QCVN 08:2023", f"MỨC {overall_tier}", "TIER", "#F59E0B"),
                unsafe_allow_html=True,
            )

        # 2D Heatmap: pH vs Temperature
        ph_range = np.linspace(6.5, 9.5, 30)
        temp_range = np.linspace(15.0, 35.0, 30)
        z_grid = np.zeros((len(temp_range), len(ph_range)))

        for i, t_val in enumerate(temp_range):
            for j, p_val in enumerate(ph_range):
                z_grid[i, j] = EmersonAmmoniaKinetics.compute_free_ammonia(
                    ammonia_total=ammonia_in, ph=float(p_val), temperature_c=float(t_val)
                )

        fig_hm = go.Figure(
            data=go.Heatmap(
                z=z_grid,
                x=ph_range,
                y=temp_range,
                colorscale="Viridis",
                colorbar={"title": "NH3 Tự Do (mg/L)"},
                hovertemplate="pH: %{x:.2f}<br>Nhiệt độ: %{y:.1f}°C<br>NH3 tự do: <b>%{z:.4f} mg/L</b><extra></extra>",
            )
        )
        # Operating Point Marker
        fig_hm.add_trace(
            go.Scatter(
                x=[ph_in],
                y=[temp_in],
                mode="markers",
                marker={"size": 14, "color": "#EF4444", "symbol": "cross", "line": {"width": 2, "color": "#FFFFFF"}},
                name="Điểm Vận Hành Hiện Tại",
            )
        )
        layout_hm = get_plotly_noc_layout(
            title="BIỂU ĐỒ NHIỆT 2 CHIỀU: NỒNG ĐỘ NH3 TỰ DO THEO PH VÀ NHIỆT ĐỘ", height=380
        )
        fig_hm.update_layout(layout_hm)
        fig_hm.update_xaxes(title="Độ pH")
        fig_hm.update_yaxes(title="Nhiệt Độ Nước (°C)")
        st.plotly_chart(fig_hm, use_container_width=True)

    # Tab 3: Sensor Diagnostics & 1D Adaptive Kalman Filter
    with tab3:
        st.markdown(
            """
            <div style="font-size: 12px; color: #94A3B8; margin-bottom: 12px;">
                Phát hiện xung nhiễu đột biến (Transient Spikes) qua phương sai trung vị tuyệt đối (MAD) và làm mịn chuỗi tín hiệu thời gian thực bằng Lọc Kalman Thích Nghi 1D.
            </div>
            """,
            unsafe_allow_html=True,
        )
        # Synthetic sensor series generation
        np.random.seed(42)
        n_points = 48
        t_steps = list(range(n_points))
        true_signal = 7.0 + 1.2 * np.sin(np.linspace(0, 3 * np.pi, n_points))
        raw_noise = true_signal + np.random.normal(0, 0.25, n_points)
        # Introduce 2 severe sensor spikes
        raw_noise[14] = 18.5  # Positive spike
        raw_noise[31] = 0.5  # Negative drop spike

        detector = SensorFaultDetector()
        spike_indices = detector.detect_spikes(raw_noise.tolist(), threshold_mad=3.5)

        kf = AdaptiveKalmanFilter1D(q=0.01, r_base=0.1, r_spike_multiplier=1000.0)
        filtered_series = kf.filter_series(raw_noise.tolist(), spike_indices=spike_indices)

        # Plotly Comparison Chart
        fig_kf = go.Figure()
        fig_kf.add_trace(
            go.Scatter(
                x=t_steps,
                y=raw_noise,
                mode="lines+markers",
                name="Số Liệu Thô (Raw Sensor Spikes)",
                line={"color": "#64748B", "width": 1},
                marker={"size": 4, "color": "#64748B"},
            )
        )
        if spike_indices:
            fig_kf.add_trace(
                go.Scatter(
                    x=[t_steps[idx] for idx in spike_indices],
                    y=[raw_noise[idx] for idx in spike_indices],
                    mode="markers",
                    name="Xung Nhiễu Phát Hiện (Spike Faults)",
                    marker={"size": 12, "color": "#EF4444", "symbol": "circle-open", "line": {"width": 2}},
                )
            )
        fig_kf.add_trace(
            go.Scatter(
                x=t_steps,
                y=filtered_series,
                mode="lines",
                name="Tín Hiệu Sau Lọc Kalman 1D",
                line={"color": "#06B6D4", "width": 3},
            )
        )
        layout_kf = get_plotly_noc_layout(title="KHỬ NHIỄU CẢM BIẾN SCADA QUA BỘ LỌC ADAPTIVE KALMAN 1D", height=380)
        fig_kf.update_layout(layout_kf)
        fig_kf.update_xaxes(title="Mốc Thời Gian (Giờ)")
        fig_kf.update_yaxes(title="DO (mg/L)")
        st.plotly_chart(fig_kf, use_container_width=True)

    # Tab 4: MLOps Drift Monitoring & Dead Letter Queue (DLQ) Healing
    with tab4:
        st.markdown(
            """
            <div style="font-size: 12px; color: #94A3B8; margin-bottom: 12px;">
                Giám sát hiện tượng trôi dạt dữ liệu (Data Drift) bằng kiểm định 2 mẫu Kolmogorov-Smirnov và chỉ số ổn định quần thể (PSI). Bàn điều khiển tự phục hồi thông điệp lỗi DLQ.
            </div>
            """,
            unsafe_allow_html=True,
        )
        # Drift Analysis
        ref_data = np.random.normal(7.2, 0.8, 100).tolist()
        curr_data = np.random.normal(5.8, 1.4, 100).tolist()

        psi_val = HydrologicalDriftMonitor.compute_psi(ref_data, curr_data, num_bins=10)
        ks_stat, ks_pval, is_drift = HydrologicalDriftMonitor.compute_ks_drift(ref_data, curr_data, alpha=0.05)

        d1, d2, d3, d4 = st.columns(4)
        with d1:
            st.markdown(
                render_scada_metric_tile(
                    "CHỈ SỐ PSI", f"{psi_val:.4f}", "SCORE", "#EF4444" if psi_val > 0.25 else "#10B981"
                ),
                unsafe_allow_html=True,
            )
        with d2:
            st.markdown(
                render_scada_metric_tile("KIỂM ĐỊNH KS STAT", f"{ks_stat:.4f}", "STAT", "#06B6D4"),
                unsafe_allow_html=True,
            )
        with d3:
            st.markdown(
                render_scada_metric_tile(
                    "KS P-VALUE", f"{ks_pval:.2e}", "P-VAL", "#EF4444" if ks_pval < 0.05 else "#10B981"
                ),
                unsafe_allow_html=True,
            )
        with d4:
            st.markdown(
                render_scada_metric_tile(
                    "KẾT LUẬN TRÔI DẠT",
                    "PHÁT HIỆN TRÔI" if is_drift else "ỔN ĐỊNH",
                    "STATUS",
                    "#EF4444" if is_drift else "#10B981",
                ),
                unsafe_allow_html=True,
            )

        st.markdown("<div style='height: 16px;'></div>", unsafe_allow_html=True)

        # DLQ Console
        st.markdown(
            """
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 13px; font-weight: 700; color: #F59E0B; margin-bottom: 8px;">
                🛠️ BÀN ĐIỀU KHIỂN TỰ PHỤC HỒI THÔNG ĐIỆP HỎNG DLQ (DEAD LETTER QUEUE SELF-HEALING)
            </div>
            """,
            unsafe_allow_html=True,
        )
        sample_broken_json = json.dumps(
            {
                "station_id": "ST_HONG_LB_01",
                "dissolved_oxygen": "6.85",  # string type anomaly
                "ph": 15.2,  # out of boundary
                "temperature": None,  # null field
                "timestamp": "2026-11-13 10:00:00",  # non-ISO format
            },
            indent=2,
        )

        dlq_col1, dlq_col2 = st.columns(2)
        with dlq_col1:
            st.markdown(
                "<div style='font-size: 11px; color: #EF4444; font-weight: bold;'>GÓI TIN LỖI DLQ BAN ĐẦU:</div>",
                unsafe_allow_html=True,
            )
            input_dlq = st.text_area("JSON DLQ Payload:", value=sample_broken_json, height=180, key="dlq_input_area")

        with dlq_col2:
            st.markdown(
                "<div style='font-size: 11px; color: #10B981; font-weight: bold;'>GÓI TIN SAU KHI SỬA CHỮA TỰ ĐỘNG:</div>",
                unsafe_allow_html=True,
            )
            try:
                engine = DLQRecoveryEngine()
                parsed_in = json.loads(input_dlq)
                healed_result = engine.heal_payload(parsed_in)
                st.json(healed_result, expanded=True)
            except Exception as e:  # noqa: BLE001
                st.error(f"Lỗi cú pháp JSON: {e}")
