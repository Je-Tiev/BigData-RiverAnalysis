"""AI 72-Hour Multi-Step Forecast Visualization with 95% Confidence Interval Ribbon."""

from typing import Any

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from src.dashboard.styles.theme import get_plotly_noc_layout


def render_forecast_chart(
    forecast_result: dict[str, Any],
    target_param: str = "dissolved_oxygen",
    height: int = 360,
) -> None:
    """Visualizes multi-step ahead trajectory predictions with Conformal 95% CI ribbon."""
    trajectory = forecast_result.get("trajectory") or forecast_result.get("forecast_points", [])
    serving_mode = forecast_result.get("_serving_mode", "UNKNOWN")

    if not trajectory:
        st.warning("Không tìm thấy dữ liệu dự báo cho trạm đã chọn.")
        return

    df_fc = pd.DataFrame(trajectory)
    # Require expected columns or map standard aliases
    time_col = "forecast_timestamp" if "forecast_timestamp" in df_fc.columns else "timestamp"
    val_col = (
        "predicted_value"
        if "predicted_value" in df_fc.columns
        else ("predicted_do" if "predicted_do" in df_fc.columns else target_param)
    )
    lower_col = (
        "confidence_lower_95"
        if "confidence_lower_95" in df_fc.columns
        else ("do_lower_95" if "do_lower_95" in df_fc.columns else "lower_bound")
    )
    upper_col = (
        "confidence_upper_95"
        if "confidence_upper_95" in df_fc.columns
        else ("do_upper_95" if "do_upper_95" in df_fc.columns else "upper_bound")
    )

    fig = go.Figure()

    # 1. Upper Confidence Bound Trace (Invisible Line, acts as upper edge of fill)
    if upper_col in df_fc.columns:
        fig.add_trace(
            go.Scatter(
                x=df_fc[time_col],
                y=df_fc[upper_col],
                mode="lines",
                line={"width": 0},
                showlegend=False,
                name="95% CI Upper",
                hoverinfo="skip",
            )
        )

    # 2. Lower Confidence Bound Trace with Shaded Fill to Upper
    if lower_col in df_fc.columns:
        fig.add_trace(
            go.Scatter(
                x=df_fc[time_col],
                y=df_fc[lower_col],
                mode="lines",
                line={"width": 0},
                fill="tonexty",
                fillcolor="rgba(6, 182, 212, 0.15)",
                name="95% Tin Cậy (Conformal Band)",
                hoverinfo="skip",
            )
        )

    # 3. Median Trajectory Prediction Line
    fig.add_trace(
        go.Scatter(
            x=df_fc[time_col],
            y=df_fc[val_col],
            mode="lines+markers",
            name="Quỹ Đạo Dự Báo (Median)",
            line={"color": "#06B6D4", "width": 2.5},
            marker={"size": 5, "color": "#06B6D4"},
            hovertemplate=("<b>%{x}</b><br>" + f"Dự báo {target_param.upper()}: <b>%{{y:.2f}}</b> mg/L<extra></extra>"),
        )
    )

    # 4. Critical Anoxia Safety Threshold (DO < 4.0 mg/L)
    if target_param == "dissolved_oxygen":
        fig.add_hline(
            y=4.0,
            line_dash="dash",
            line_color="#EF4444",
            line_width=1.5,
            annotation_text="Ngưỡng Ngạt Thủy Sinh (4.0 mg/L)",
            annotation_position="bottom right",
            annotation_font={"size": 10, "color": "#EF4444", "family": "JetBrains Mono, monospace"},
        )

    # Serving Mode Annotation Pill
    mode_color = "#10B981" if "REMOTE" in serving_mode else "#F59E0B"
    title_text = (
        f"QUỸ ĐẠO DỰ BÁO {target_param.upper()} 72H | "
        f"<span style='font-size:10px; color:{mode_color};'>[{serving_mode}]</span>"
    )

    layout = get_plotly_noc_layout(title=title_text, height=height)
    fig.update_layout(layout)
    st.plotly_chart(fig, use_container_width=True)
