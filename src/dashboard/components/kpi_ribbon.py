"""Executive Basin KPI Ribbon Component with Tabular Numerals."""

from typing import Any

import streamlit as st

from src.dashboard.styles.colors import get_wqi_color, get_wqi_tier_label
from src.dashboard.styles.theme import render_scada_metric_tile


def render_kpi_ribbon(
    network_state: list[dict[str, Any]],
    streaming_latency_ms: int = 820,
) -> None:
    """Renders the top executive 5-metric SCADA ribbon without layout jitter."""
    total_stations = len(network_state)
    active_stations = sum(1 for s in network_state if s.get("dissolved_oxygen") is not None)

    # Average WQI across active stations
    wqi_scores = [
        float(s.get("vn_wqi_score") or s.get("ccme_score", 0.0))
        for s in network_state
        if (s.get("vn_wqi_score") or s.get("ccme_score")) is not None
    ]
    avg_wqi = sum(wqi_scores) / len(wqi_scores) if wqi_scores else 0.0
    wqi_color = get_wqi_color(avg_wqi)
    wqi_label = get_wqi_tier_label(avg_wqi)

    # Compliance calculation (non-anomalous)
    compliant_count = sum(1 for s in network_state if not s.get("is_anomaly", False))
    compliance_pct = (compliant_count / total_stations * 100.0) if total_stations > 0 else 0.0

    # Active incidents count
    active_incidents = sum(1 for s in network_state if s.get("is_anomaly", False))

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.markdown(
            render_scada_metric_tile(
                label="TỔNG SỐ TRẠM MẠNG LƯỚI",
                value=f"{active_stations}/{total_stations}",
                unit="TRẠM",
                delta="100% Hoạt động",
            ),
            unsafe_allow_html=True,
        )
    with c2:
        st.markdown(
            render_scada_metric_tile(
                label="CHỈ SỐ VN_WQI TOÀN MẠNG",
                value=f"{avg_wqi:.1f}",
                unit="ĐIỂM",
                delta=f"[{wqi_label.upper()}]",
                status_color=wqi_color,
            ),
            unsafe_allow_html=True,
        )
    with c3:
        st.markdown(
            render_scada_metric_tile(
                label="TUÂN THỦ QCVN 08:2023",
                value=f"{compliance_pct:.1f}",
                unit="%",
                delta=f"{compliant_count}/{total_stations} Trạm Đạt",
                status_color="#10B981" if compliance_pct >= 80 else "#F59E0B",
            ),
            unsafe_allow_html=True,
        )
    with c4:
        st.markdown(
            render_scada_metric_tile(
                label="SỰ CỐ ĐANG KÍCH HOẠT",
                value=f"{active_incidents:02d}",
                unit="CẢNH BÁO",
                delta="Cần xác nhận SOP" if active_incidents > 0 else "Bình thường",
                status_color="#EF4444" if active_incidents > 0 else "#10B981",
            ),
            unsafe_allow_html=True,
        )
    with c5:
        st.markdown(
            render_scada_metric_tile(
                label="ĐỘ TRỄ INGESTION (SLA)",
                value=f"{streaming_latency_ms}",
                unit="ms",
                delta="Kafka Ingest: 1.2k evt/s",
                status_color="#06B6D4",
            ),
            unsafe_allow_html=True,
        )
