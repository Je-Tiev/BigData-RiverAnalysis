"""Styles package initialization."""

from src.dashboard.styles.colors import (
    BORDER_SUBTLE,
    BRAND_CYAN,
    CANVAS_BG,
    PARAMETER_COLOR_MAP,
    QCVN_TIER_COLORS,
    SEVERITY_COLORS,
    SURFACE_CARD,
    TEXT_PRIMARY,
    TEXT_SECONDARY,
    WQI_5TIER_PALETTE,
    get_severity_badge_html,
    get_severity_color,
    get_wqi_color,
    get_wqi_tier_label,
)
from src.dashboard.styles.theme import (
    apply_noc_theme,
    get_plotly_noc_layout,
    render_scada_metric_tile,
    render_technical_badge,
)

__all__ = [
    "BORDER_SUBTLE",
    "BRAND_CYAN",
    "CANVAS_BG",
    "PARAMETER_COLOR_MAP",
    "QCVN_TIER_COLORS",
    "SEVERITY_COLORS",
    "SURFACE_CARD",
    "TEXT_PRIMARY",
    "TEXT_SECONDARY",
    "WQI_5TIER_PALETTE",
    "apply_noc_theme",
    "get_plotly_noc_layout",
    "get_severity_badge_html",
    "get_severity_color",
    "get_wqi_color",
    "get_wqi_tier_label",
    "render_scada_metric_tile",
    "render_technical_badge",
]
