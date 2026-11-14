"""Non-blocking Infrastructure Health Probes and 1-Click Launchers Component."""

import socket
import urllib.request
from typing import Any

import streamlit as st

INFRA_TARGETS = {
    "FastAPI :8000": {
        "type": "http",
        "url": "http://localhost:8000/health",
        "link": "http://localhost:8000/docs",
    },
    "Grafana :30003": {
        "type": "http",
        "url": "http://localhost:30003/api/health",
        "link": "http://localhost:30003",
    },
    "Airflow :8085": {
        "type": "http",
        "url": "http://localhost:8085/health",
        "link": "http://localhost:8085",
    },
    "Kafka UI :8082": {
        "type": "socket",
        "host": "127.0.0.1",
        "port": 8082,
        "link": "http://localhost:8082",
    },
    "MinIO :30001": {
        "type": "socket",
        "host": "127.0.0.1",
        "port": 30001,
        "link": "http://localhost:30001",
    },
    "Spark :9090": {
        "type": "socket",
        "host": "127.0.0.1",
        "port": 9090,
        "link": "http://localhost:9090",
    },
}


def probe_single_service(config: dict[str, Any], timeout_sec: float = 1.0) -> tuple[str, str]:
    """Probes a single infrastructure service and returns (status, direct_link)."""
    target_link = config.get("link", "#")
    try:
        if config["type"] == "http":
            req = urllib.request.Request(config["url"], headers={"User-Agent": "SCADA-Probe/2.0"})
            with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
                if resp.status == 200:
                    return "ACTIVE", target_link
                return "DEGRADED", target_link
        elif config["type"] == "socket":
            with socket.create_connection((config["host"], config["port"]), timeout=timeout_sec):
                return "ACTIVE", target_link
    except Exception:  # noqa: BLE001
        return "UNREACHABLE", target_link
    return "UNREACHABLE", target_link


def probe_all_infrastructure_services(timeout_sec: float = 1.0) -> dict[str, tuple[str, str]]:
    """Probes all 6 infrastructure endpoints."""
    results = {}
    for name, cfg in INFRA_TARGETS.items():
        results[name] = probe_single_service(cfg, timeout_sec=timeout_sec)
    return results


def render_infra_status_badges(
    statuses: dict[str, tuple[str, str]] | None = None,
    timeout_sec: float = 1.0,
) -> None:
    """Renders 6 horizontal status pills with direct launch links."""
    active_statuses = statuses or probe_all_infrastructure_services(timeout_sec=timeout_sec)

    cols = st.columns(6)
    for idx, (name, (status, link)) in enumerate(active_statuses.items()):
        color = "#10B981" if status == "ACTIVE" else ("#F59E0B" if status == "DEGRADED" else "#EF4444")
        bg = "#052E16" if status == "ACTIVE" else ("#451A03" if status == "DEGRADED" else "#450A0A")
        with cols[idx]:
            st.markdown(
                f"""
                <a href="{link}" target="_blank" style="text-decoration: none;">
                    <div style="background-color: {bg}; border: 1px solid {color}; border-radius: 2px; padding: 4px 6px; text-align: center; transition: all 0.15s ease;">
                        <div style="font-family: 'JetBrains Mono', monospace; font-size: 10px; font-weight: 700; color: #F8FAFC;">{name}</div>
                        <div style="font-family: 'JetBrains Mono', monospace; font-size: 9px; color: {color}; font-weight: 700;">● [{status}]</div>
                    </div>
                </a>
                """,
                unsafe_allow_html=True,
            )
