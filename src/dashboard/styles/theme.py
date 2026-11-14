"""Theme Injection and Standard Plotly NOC Layout Configuration."""

import os
from typing import Any

import streamlit as st

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
CSS_PATH = os.path.join(CURRENT_DIR, "custom_noc.css")


def apply_noc_theme() -> None:
    """Injects the industrial NOC Dark Slate CSS stylesheet into Streamlit."""
    if os.path.exists(CSS_PATH):
        with open(CSS_PATH, encoding="utf-8") as f:
            css_content = f.read()
        st.markdown(f"<style>{css_content}</style>", unsafe_allow_html=True)


def get_plotly_noc_layout(title: str = "", height: int = 350, **kwargs: Any) -> dict[str, Any]:
    """Returns a standardized dictionary layout for Plotly figures in NOC dark mode."""
    layout: dict[str, Any] = {
        "template": "plotly_dark",
        "paper_bgcolor": "#0F172A",
        "plot_bgcolor": "#1E293B",
        "height": height,
        "margin": {"l": 50, "r": 20, "t": 40 if title else 25, "b": 35},
        "font": {
            "family": "Inter, -apple-system, sans-serif",
            "size": 11,
            "color": "#94A3B8",
        },
        "title": {
            "text": title,
            "font": {"size": 13, "color": "#F8FAFC", "family": "Inter, sans-serif"},
            "x": 0.02,
            "xanchor": "left",
        }
        if title
        else None,
        "xaxis": {
            "gridcolor": "#334155",
            "gridwidth": 1,
            "zeroline": False,
            "showline": True,
            "linecolor": "#334155",
            "tickfont": {
                "family": "JetBrains Mono, monospace",
                "size": 10,
                "color": "#94A3B8",
            },
        },
        "yaxis": {
            "gridcolor": "#334155",
            "gridwidth": 1,
            "zeroline": False,
            "showline": True,
            "linecolor": "#334155",
            "tickfont": {
                "family": "JetBrains Mono, monospace",
                "size": 10,
                "color": "#94A3B8",
            },
        },
        "hoverlabel": {
            "bgcolor": "#0F172A",
            "bordercolor": "#06B6D4",
            "font": {
                "family": "JetBrains Mono, monospace",
                "size": 11,
                "color": "#F8FAFC",
            },
        },
        "legend": {
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "right",
            "x": 1.0,
            "font": {"size": 10, "color": "#94A3B8"},
        },
    }
    # Update with any caller overrides
    layout.update(kwargs)
    return layout


def render_scada_metric_tile(
    label: str,
    value: Any,
    unit: str = "",
    delta: str | None = None,
    status_color: str | None = None,
) -> str:
    """Returns HTML markup for a zero-jitter SCADA metric tile."""
    color_style = f"color: {status_color};" if status_color else "color: #F8FAFC;"
    unit_span = (
        f'<span style="font-size: 13px; color: #94A3B8; font-weight: normal; margin-left: 4px;">{unit}</span>'
        if unit
        else ""
    )
    delta_div = f'<div class="scada-metric-sub">{delta}</div>' if delta else ""
    return f"""
    <div class="scada-metric-tile">
        <div class="scada-metric-label">{label}</div>
        <div class="scada-metric-val" style="{color_style}">{value}{unit_span}</div>
        {delta_div}
    </div>
    """


def render_technical_badge(category: str, label: str) -> str:
    """Returns a standardized monochrome technical category badge HTML."""
    return (
        f'<span class="badge-mono" style="font-family: \'JetBrains Mono\', monospace; '
        f"font-size: 11px; background-color: #1E293B; border: 1px solid #334155; "
        f'color: #06B6D4; padding: 2px 6px; border-radius: 2px; font-weight: 700;">'
        f"[{category.upper()}]</span> "
        f'<span style="font-weight: 600; color: #F8FAFC;">{label}</span>'
    )
