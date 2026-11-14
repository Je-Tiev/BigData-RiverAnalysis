"""Industrial Dark Slate NOC Color Palette & Token Definitions.

Standardized for ISA-101 compliance, WCAG AAA contrast ratio,
and 5-tier Water Quality Index semantics (Decision 1460/QD-TCMT).
"""

from typing import Final

# ---------------------------------------------------------------------------
# 1. Canvas & Surface Design Tokens (Slate Scale)
# ---------------------------------------------------------------------------
CANVAS_BG: Final[str] = "#0F172A"  # Slate-900: Deep canvas background
SURFACE_CARD: Final[str] = "#1E293B"  # Slate-800: Metric card & container surface
SURFACE_HOVER: Final[str] = "#334155"  # Slate-700: Hover state for rows and cards
BORDER_SUBTLE: Final[str] = "#334155"  # Slate-700: 1px technical border
BORDER_FOCUS: Final[str] = "#06B6D4"  # Cyan-500: Active station focus border
TEXT_PRIMARY: Final[str] = "#F8FAFC"  # Slate-50: Primary telemetry readings
TEXT_SECONDARY: Final[str] = "#94A3B8"  # Slate-400: Metadata labels, units
TEXT_MUTED: Final[str] = "#64748B"  # Slate-500: Timestamps, footnotes
BRAND_CYAN: Final[str] = "#06B6D4"  # Cyan-500: Streamline pulse & telemetry line
BRAND_GLOW: Final[str] = "rgba(6, 182, 212, 0.15)"  # 95% Confidence interval band

# ---------------------------------------------------------------------------
# 2. 5-Tier WQI Palette (VN_WQI QĐ 1460/QĐ-TCMT & CCME WQI)
# ---------------------------------------------------------------------------
WQI_5TIER_PALETTE: Final[dict[str, dict[str, str]]] = {
    "VERY_GOOD": {
        "range": "91-100",
        "label": "Rất tốt",
        "hex": "#10B981",  # Emerald-500
        "rgb": "rgb(16, 185, 129)",
        "badge_bg": "#052E16",
        "badge_border": "#16A34A",
        "badge_text": "#86EFAC",
    },
    "GOOD": {
        "range": "76-90",
        "label": "Tốt",
        "hex": "#06B6D4",  # Cyan-500
        "rgb": "rgb(6, 182, 212)",
        "badge_bg": "#083344",
        "badge_border": "#0891B2",
        "badge_text": "#67E8F9",
    },
    "MODERATE": {
        "range": "51-75",
        "label": "Trung bình",
        "hex": "#F59E0B",  # Amber-500
        "rgb": "rgb(245, 158, 11)",
        "badge_bg": "#451A03",
        "badge_border": "#D97706",
        "badge_text": "#FDE68A",
    },
    "POOR": {
        "range": "26-50",
        "label": "Kém",
        "hex": "#F97316",  # Orange-500
        "rgb": "rgb(249, 115, 22)",
        "badge_bg": "#431407",
        "badge_border": "#EA580C",
        "badge_text": "#FDBA74",
    },
    "POLLUTED": {
        "range": "0-25",
        "label": "Ô nhiễm nặng",
        "hex": "#EF4444",  # Red-500
        "rgb": "rgb(239, 68, 68)",
        "badge_bg": "#450A0A",
        "badge_border": "#DC2626",
        "badge_text": "#FCA5A5",
    },
}

# QCVN 08:2023 Tier Color Mapping
QCVN_TIER_COLORS: Final[dict[str, str]] = {
    "A": "#10B981",
    "B": "#06B6D4",
    "C": "#F59E0B",
    "D": "#EF4444",
}

# Operational Severity Colors
SEVERITY_COLORS: Final[dict[str, str]] = {
    "CRITICAL": "#EF4444",
    "HIGH": "#F97316",
    "MEDIUM": "#F59E0B",
    "NOMINAL": "#10B981",
    "INFO": "#06B6D4",
}

# ---------------------------------------------------------------------------
# 3. Parameter Trace Colors (18 Physicochemical Parameters)
# ---------------------------------------------------------------------------
PARAMETER_COLOR_MAP: Final[dict[str, str]] = {
    # Core Parameters
    "dissolved_oxygen": "#06B6D4",  # Cyan (Oxygenation)
    "ph": "#10B981",  # Emerald Green (Acidity/Alkalinity balance)
    "temperature": "#F59E0B",  # Amber (Thermal cycle)
    "conductivity": "#6366F1",  # Indigo (Electrical conductance)
    "ammonia": "#EF4444",  # Red (Toxic ammonia)
    "bod5": "#A855F7",  # Purple (Biological oxygen demand)
    "chloride": "#0EA5E9",  # Sky Blue (Saline ions)
    "total_hardness": "#64748B",  # Slate Grey (Mineral hardness)
    "turbidity_ntu": "#EAB308",  # Yellow (Suspended sediments)
    "flow_rate_m3s": "#38BDF8",  # Light Blue (Hydraulic discharge)
    # Schema v1.3.0 Expanded Parameters
    "cod": "#EC4899",  # Pink (Chemical oxygen demand)
    "tss": "#D97706",  # Ochre / Terracotta (Total suspended solids)
    "nitrate_no3": "#3B82F6",  # Blue (Nitrate nutrient)
    "phosphate_po4": "#14B8A6",  # Teal (Phosphate nutrient)
    "coliform": "#F43F5E",  # Rose Red (Pathogenic microbes)
    "salinity": "#8B5CF6",  # Violet (Tidal saltwater intrusion)
    "alkalinity": "#2DD4BF",  # Mint Green (Buffering capacity)
    "true_colour": "#A1A1AA",  # Zinc Grey (Humic dissolved color)
}


# ---------------------------------------------------------------------------
# 4. Helper Functions
# ---------------------------------------------------------------------------
def get_wqi_color(score: float | None) -> str:
    """Returns the standardized hex color for a given WQI score (0-100)."""
    if score is None:
        return "#64748B"
    if score >= 91.0:
        return WQI_5TIER_PALETTE["VERY_GOOD"]["hex"]
    if score >= 76.0:
        return WQI_5TIER_PALETTE["GOOD"]["hex"]
    if score >= 51.0:
        return WQI_5TIER_PALETTE["MODERATE"]["hex"]
    if score >= 26.0:
        return WQI_5TIER_PALETTE["POOR"]["hex"]
    return WQI_5TIER_PALETTE["POLLUTED"]["hex"]


def get_wqi_tier_label(score: float | None) -> str:
    """Returns the descriptive Vietnamese label for a given WQI score."""
    if score is None:
        return "Chưa xác định"
    if score >= 91.0:
        return WQI_5TIER_PALETTE["VERY_GOOD"]["label"]
    if score >= 76.0:
        return WQI_5TIER_PALETTE["GOOD"]["label"]
    if score >= 51.0:
        return WQI_5TIER_PALETTE["MODERATE"]["label"]
    if score >= 26.0:
        return WQI_5TIER_PALETTE["POOR"]["label"]
    return WQI_5TIER_PALETTE["POLLUTED"]["label"]


def get_severity_color(severity: str) -> str:
    """Returns the hex color code for a given alert severity."""
    return SEVERITY_COLORS.get(severity.upper(), "#94A3B8")


def get_severity_badge_html(severity: str) -> str:
    """Generates an HTML technical badge string for incident cards."""
    sev = severity.upper()
    if sev == "CRITICAL":
        cfg = WQI_5TIER_PALETTE["POLLUTED"]
    elif sev == "HIGH":
        cfg = WQI_5TIER_PALETTE["POOR"]
    elif sev == "MEDIUM":
        cfg = WQI_5TIER_PALETTE["MODERATE"]
    else:
        cfg = WQI_5TIER_PALETTE["VERY_GOOD"]

    return (
        f'<span style="background-color: {cfg["badge_bg"]}; '
        f"border: 1px solid {cfg['badge_border']}; "
        f"color: {cfg['badge_text']}; "
        f"font-family: 'JetBrains Mono', monospace; "
        f"font-size: 11px; font-weight: 700; "
        f'padding: 2px 8px; border-radius: 2px; text-transform: uppercase;">'
        f"[{sev}]</span>"
    )
