"""GIS Network Spatial Command Map Component for SCADA Operations."""

from typing import Any

import folium
from folium.plugins import Fullscreen
from streamlit_folium import st_folium

from src.dashboard.styles.colors import get_wqi_color


def render_gis_network_map(
    stations: list[dict[str, Any]],
    selected_station_id: str | None = None,
    height: int = 560,
    zoom_start: int = 6,
) -> Any:
    """Renders the CartoDB DarkMatter GIS map with 5-tier WQI markers and anomaly pulse rings."""
    # Center coordinates of Vietnam river system
    vietnam_center = [16.0, 107.5]

    try:
        m = folium.Map(
            location=vietnam_center,
            zoom_start=zoom_start,
            tiles="cartodb dark_matter",
            control_scale=True,
        )
    except Exception:  # noqa: BLE001
        # Fallback to standard OpenStreetMap if dark_matter tile CDN is unreachable
        m = folium.Map(
            location=vietnam_center,
            zoom_start=zoom_start,
            tiles="OpenStreetMap",
            control_scale=True,
        )

    Fullscreen(position="topright").add_to(m)

    for st_rec in stations:
        lat = float(st_rec.get("latitude", 0.0))
        lon = float(st_rec.get("longitude", 0.0))
        station_id = st_rec.get("station_id", "")
        station_name = st_rec.get("station_name", station_id)
        river_name = st_rec.get("river_name", "")
        wqi = st_rec.get("vn_wqi_score") or st_rec.get("ccme_score", 80.0)
        is_anomaly = bool(st_rec.get("is_anomaly", False))

        pin_color = get_wqi_color(wqi)
        is_selected = station_id == selected_station_id

        border_style = "3px solid #F8FAFC;" if is_selected else f"1.5px solid {pin_color};"
        pulse_class = "pulse-marker" if is_anomaly else ""

        anomaly_div = (
            f'<div style="margin-top: 6px; padding: 4px; background-color: #450A0A; border: 1px solid #DC2626; '
            f'color: #FCA5A5; font-size: 9px; border-radius: 2px;">⚠️ DỊ THƯỜNG: '
            f"{st_rec.get('anomaly_reason', 'Vượt ngưỡng an toàn')}</div>"
            if is_anomaly
            else ""
        )

        # Rich SCADA Popup HTML with Monospace Tabular Numerals
        popup_html = f"""
        <div style="font-family: 'Segoe UI', Tahoma, sans-serif; font-size: 11px; color: #F8FAFC; background-color: #0F172A; padding: 8px 12px; border: 1px solid #334155; border-radius: 2px; width: 220px;">
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 10px; color: #06B6D4; font-weight: 700;">[{station_id}]</div>
            <div style="font-size: 13px; font-weight: 700; color: #F8FAFC; margin-bottom: 2px;">{station_name}</div>
            <div style="color: #94A3B8; font-size: 10px; margin-bottom: 8px;">{river_name}</div>

            <div style="background-color: #1E293B; border: 1px solid #334155; padding: 6px 8px; border-radius: 2px; margin-bottom: 6px;">
                <span style="color: #94A3B8;">VN_WQI:</span>
                <strong style="color: {pin_color}; font-family: 'JetBrains Mono', monospace; font-size: 13px;">{wqi:.1f}</strong>
                <span style="font-size: 10px; color: {pin_color};">[{st_rec.get("vn_wqi_label", "N/A")}]</span>
            </div>

            <table style="width: 100%; border-collapse: collapse; font-family: 'JetBrains Mono', monospace; font-size: 10px;">
                <tr><td style="color: #94A3B8;">DO:</td><td style="text-align: right; color: #F8FAFC;">{st_rec.get("dissolved_oxygen", "N/A")} mg/L</td></tr>
                <tr><td style="color: #94A3B8;">pH:</td><td style="text-align: right; color: #F8FAFC;">{st_rec.get("ph", "N/A")}</td></tr>
                <tr><td style="color: #94A3B8;">Temp:</td><td style="text-align: right; color: #F8FAFC;">{st_rec.get("temperature", "N/A")} °C</td></tr>
                <tr><td style="color: #94A3B8;">COD:</td><td style="text-align: right; color: #F8FAFC;">{st_rec.get("cod", "N/A")} mg/L</td></tr>
                <tr><td style="color: #94A3B8;">Ammonia:</td><td style="text-align: right; color: #F8FAFC;">{st_rec.get("ammonia", "N/A")} mg/L</td></tr>
                <tr><td style="color: #94A3B8;">Salinity:</td><td style="text-align: right; color: #F8FAFC;">{st_rec.get("salinity", "N/A")} ppt</td></tr>
            </table>

            {anomaly_div}
        </div>
        """

        # Custom HTML DivIcon Pin
        icon_html = f"""
        <div class="{pulse_class}" style="
            background-color: {pin_color};
            width: 16px;
            height: 16px;
            border-radius: 50%;
            border: {border_style}
            box-shadow: 0 0 8px {pin_color};
        "></div>
        """

        marker = folium.Marker(
            location=[lat, lon],
            icon=folium.DivIcon(
                icon_size=(16, 16),
                icon_anchor=(8, 8),
                html=icon_html,
            ),
            popup=folium.Popup(popup_html, max_width=260),
            tooltip=f"[{station_id}] {station_name} | WQI: {wqi:.1f}",
        )
        marker.add_to(m)

    return st_folium(
        m,
        height=height,
        use_container_width=True,
        returned_objects=["last_object_clicked"],
    )
