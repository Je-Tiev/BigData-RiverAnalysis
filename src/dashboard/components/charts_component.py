"""Plotly Multi-Trace Time Series Component for Telemetry Visualization."""

from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.contracts.river_schema import CRITICAL_THRESHOLDS
from src.dashboard.styles.colors import PARAMETER_COLOR_MAP
from src.dashboard.styles.theme import get_plotly_noc_layout


def create_telemetry_trace_figure(
    df: pd.DataFrame,
    parameter: str,
    title: str = "",
    height: int = 300,
) -> go.Figure:
    """Builds a single parameter Plotly figure with safety thresholds."""
    fig = go.Figure()

    if parameter not in df.columns:
        return fig

    trace_color = PARAMETER_COLOR_MAP.get(parameter, "#06B6D4")

    # Main telemetry line
    fig.add_trace(
        go.Scatter(
            x=df["timestamp"],
            y=df[parameter],
            mode="lines+markers",
            name=parameter.replace("_", " ").title(),
            line={"color": trace_color, "width": 2},
            marker={"size": 4, "color": trace_color},
            hovertemplate=("<b>%{x}</b><br>" + f"{parameter}: " + "<b>%{y:.2f}</b><extra></extra>"),
        )
    )

    # Overlay critical hazard threshold if defined
    threshold_cfg = CRITICAL_THRESHOLDS.get(parameter)
    if threshold_cfg:
        if "max" in threshold_cfg:
            fig.add_hline(
                y=threshold_cfg["max"],
                line_dash="dash",
                line_color="#EF4444",
                line_width=1.5,
                annotation_text=f"Hazard Max: {threshold_cfg['max']}",
                annotation_position="top right",
                annotation_font={"size": 9, "color": "#EF4444", "family": "JetBrains Mono, monospace"},
            )
        if "min" in threshold_cfg:
            fig.add_hline(
                y=threshold_cfg["min"],
                line_dash="dash",
                line_color="#EF4444",
                line_width=1.5,
                annotation_text=f"Hazard Min: {threshold_cfg['min']}",
                annotation_position="bottom right",
                annotation_font={"size": 9, "color": "#EF4444", "family": "JetBrains Mono, monospace"},
            )

    layout_dict = get_plotly_noc_layout(title=title, height=height)
    fig.update_layout(layout_dict)
    return fig


def render_telemetry_grid(
    hist_data: list[dict[str, Any]],
    parameters: list[str] | None = None,
    height: int = 280,
) -> None:
    """Renders a responsive 2-column grid of Plotly charts for specified parameters."""
    if not hist_data:
        st.info("Chưa có dữ liệu viễn thám chuỗi thời gian.")
        return

    df = pd.DataFrame(hist_data)
    target_params = parameters or [
        "dissolved_oxygen",
        "ph",
        "temperature",
        "ammonia",
        "cod",
        "tss",
        "salinity",
        "coliform",
    ]

    available_params = [p for p in target_params if p in df.columns]

    for i in range(0, len(available_params), 2):
        cols = st.columns(2)
        p1 = available_params[i]
        with cols[0]:
            fig1 = create_telemetry_trace_figure(df, p1, title=p1.replace("_", " ").upper(), height=height)
            st.plotly_chart(fig1, use_container_width=True)

        if i + 1 < len(available_params):
            p2 = available_params[i + 1]
            with cols[1]:
                fig2 = create_telemetry_trace_figure(df, p2, title=p2.replace("_", " ").upper(), height=height)
                st.plotly_chart(fig2, use_container_width=True)
