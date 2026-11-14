"""Incident Kanban Card and Dual-Layer Root Cause Analysis Component."""

import hashlib
from collections.abc import Callable
from typing import Any

import streamlit as st

from src.dashboard.styles.colors import get_severity_badge_html


def render_incident_card(
    incident: dict[str, Any],
    on_acknowledge: Callable[[str], None] | None = None,
    on_dispatch: Callable[[str], None] | None = None,
    key_prefix: str = "inc",
) -> None:
    """Renders a high-density SCADA incident card with Dual-Layer RCA and SOP actions."""
    inc_id = incident.get("incident_id", f"INC-{incident.get('station_id', 'UNKNOWN')}")
    station_id = incident.get("station_id", "UNKNOWN")
    station_name = incident.get("station_name", station_id)
    severity = incident.get("severity", "HIGH").upper()
    timestamp = incident.get("timestamp", "N/A")
    status = incident.get("status", "NEW").upper()
    reason = incident.get("anomaly_reason", "Phát hiện thông số vượt ngưỡng an toàn.")
    incident.get("metrics_snapshot", {})

    badge_html = get_severity_badge_html(severity)
    sha_seal = hashlib.sha256(f"{inc_id}_{station_id}_{timestamp}".encode()).hexdigest()[:10]

    with st.container():
        st.markdown(
            f"""
            <div class="scada-metric-tile" style="border-left: 3px solid {"#EF4444" if severity == "CRITICAL" else "#F97316"};">
                <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 6px;">
                    <div>{badge_html} <strong style="font-family: 'JetBrains Mono', monospace; font-size: 13px; color: #F8FAFC; margin-left: 6px;">{inc_id}</strong></div>
                    <div style="font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #64748B;">TRẠNG THÁI: <span style="color: #06B6D4; font-weight: 700;">[{status}]</span></div>
                </div>
                <div style="font-size: 13px; font-weight: 600; color: #F8FAFC;">{station_name} <span style="color: #94A3B8; font-weight: normal; font-size: 11px;">({station_id})</span></div>
                <div style="font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #94A3B8; margin-bottom: 6px;">MỐC GIỜ: {timestamp}</div>
                <div style="font-size: 11px; color: #E2E8F0; background-color: #0F172A; border: 1px solid #334155; padding: 6px 8px; border-radius: 2px; margin-bottom: 6px;">
                    <strong>RCA (Nguyên nhân):</strong> {reason}
                </div>
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <span style="font-family: monospace; font-size: 9px; color: #64748B;">SHA-256 SEAL: {sha_seal}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        col1, col2 = st.columns(2)
        with col1:
            if st.button("TIẾP NHẬN [ACK]", key=f"{key_prefix}_ack_{inc_id}", use_container_width=True):
                if on_acknowledge:
                    on_acknowledge(inc_id)
                else:
                    st.success(f"Đã tiếp nhận {inc_id}")
        with col2:
            if st.button("ĐIỀU PHỐI [DISPATCH]", key=f"{key_prefix}_disp_{inc_id}", use_container_width=True):
                if on_dispatch:
                    on_dispatch(inc_id)
                else:
                    st.info(f"Đã mở lệnh điều phối cho {inc_id}")
