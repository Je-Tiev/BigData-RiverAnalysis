"""Unit and Regression Tests for Stage 2 (Styles, Services & SCADA Atomic Components).

Validates:
- P8-05: Styles tokens, 5-tier WQI, Plotly NOC dark layout, SCADA metric tiles.
- P8-06: FastAPI Client 3-state Circuit Breaker, Telemetry Service snapshots, Report Export Service.
- P8-07: SCADA components (telemetry grid figure, infrastructure probes).
"""

import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.dashboard.components.charts_component import create_telemetry_trace_figure
from src.dashboard.components.infra_status_badge import INFRA_TARGETS, probe_all_infrastructure_services
from src.dashboard.services.api_client import CircuitBreaker, CircuitState, FastAPIClient
from src.dashboard.services.export_service import ReportExportService
from src.dashboard.services.telemetry_service import TelemetryService
from src.dashboard.styles.colors import (
    PARAMETER_COLOR_MAP,
    WQI_5TIER_PALETTE,
    get_severity_badge_html,
    get_severity_color,
    get_wqi_color,
    get_wqi_tier_label,
)
from src.dashboard.styles.theme import get_plotly_noc_layout, render_scada_metric_tile, render_technical_badge


class TestStylesPackage(unittest.TestCase):
    """Test suite for styles tokens, palettes, and HTML helper renderers."""

    def test_wqi_5tier_palette_coverage(self):
        for tier in ["VERY_GOOD", "GOOD", "MODERATE", "POOR", "POLLUTED"]:
            self.assertIn(tier, WQI_5TIER_PALETTE)
            cfg = WQI_5TIER_PALETTE[tier]
            self.assertTrue(cfg["hex"].startswith("#"))
            self.assertIn("label", cfg)

    def test_parameter_color_map_contains_all_18_parameters(self):
        expected_params = [
            "dissolved_oxygen",
            "ph",
            "temperature",
            "conductivity",
            "ammonia",
            "bod5",
            "chloride",
            "total_hardness",
            "turbidity_ntu",
            "flow_rate_m3s",
            "cod",
            "tss",
            "nitrate_no3",
            "phosphate_po4",
            "coliform",
            "salinity",
            "alkalinity",
            "true_colour",
        ]
        for p in expected_params:
            self.assertIn(p, PARAMETER_COLOR_MAP, f"Missing parameter {p} in PARAMETER_COLOR_MAP")

    def test_get_wqi_color_and_tier_label(self):
        self.assertEqual(get_wqi_color(95.0), WQI_5TIER_PALETTE["VERY_GOOD"]["hex"])
        self.assertEqual(get_wqi_tier_label(95.0), "Rất tốt")

        self.assertEqual(get_wqi_color(80.0), WQI_5TIER_PALETTE["GOOD"]["hex"])
        self.assertEqual(get_wqi_tier_label(80.0), "Tốt")

        self.assertEqual(get_wqi_color(60.0), WQI_5TIER_PALETTE["MODERATE"]["hex"])
        self.assertEqual(get_wqi_tier_label(60.0), "Trung bình")

        self.assertEqual(get_wqi_color(35.0), WQI_5TIER_PALETTE["POOR"]["hex"])
        self.assertEqual(get_wqi_tier_label(35.0), "Kém")

        self.assertEqual(get_wqi_color(10.0), WQI_5TIER_PALETTE["POLLUTED"]["hex"])
        self.assertEqual(get_wqi_tier_label(10.0), "Ô nhiễm nặng")

        self.assertEqual(get_wqi_color(None), "#64748B")
        self.assertEqual(get_wqi_tier_label(None), "Chưa xác định")

    def test_severity_color_and_badge_html(self):
        self.assertEqual(get_severity_color("CRITICAL"), "#EF4444")
        self.assertEqual(get_severity_color("HIGH"), "#F97316")
        badge = get_severity_badge_html("CRITICAL")
        self.assertIn("[CRITICAL]", badge)
        self.assertIn("JetBrains Mono", badge)

    def test_get_plotly_noc_layout_and_render_tiles(self):
        layout = get_plotly_noc_layout("TEST CHART", height=400)
        self.assertEqual(layout["paper_bgcolor"], "#0F172A")
        self.assertEqual(layout["plot_bgcolor"], "#1E293B")
        self.assertEqual(layout["height"], 400)

        tile_html = render_scada_metric_tile("DO", 7.5, unit="mg/L", delta="-0.2", status_color="#10B981")
        self.assertIn("scada-metric-tile", tile_html)
        self.assertIn("7.5", tile_html)
        self.assertIn("mg/L", tile_html)

        badge_html = render_technical_badge("GIS", "Trạm Hà Nội")
        self.assertIn("[GIS]", badge_html)
        self.assertIn("Trạm Hà Nội", badge_html)


class TestServicesPackage(unittest.TestCase):
    """Test suite for TelemetryService, FastAPIClient, and ReportExportService."""

    def setUp(self):
        self.tel_service = TelemetryService.get_instance()

    def test_telemetry_service_network_state_and_series(self):
        state = self.tel_service.get_current_network_state()
        self.assertEqual(len(state), 10)
        first = state[0]
        self.assertIn("station_id", first)
        self.assertIn("vn_wqi_score", first)
        self.assertIn("dissolved_oxygen", first)

        meta = self.tel_service.get_station_metadata(first["station_id"])
        self.assertIsNotNone(meta)
        self.assertEqual(meta["station_id"], first["station_id"])

        series = self.tel_service.generate_recent_series(first, hours=12)
        self.assertEqual(len(series), 13)
        self.assertGreater(series[0]["dissolved_oxygen"], 0.0)

    def test_circuit_breaker_state_transitions(self):
        cb = CircuitBreaker(failure_threshold=2, recovery_timeout_sec=0.1)
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertTrue(cb.can_attempt_call())

        # First failure
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.CLOSED)

        # Second failure -> trip to OPEN
        cb.record_failure()
        self.assertEqual(cb.state, CircuitState.OPEN)
        self.assertFalse(cb.can_attempt_call())

        # Wait for recovery timeout
        import time

        time.sleep(0.15)
        self.assertTrue(cb.can_attempt_call())
        self.assertEqual(cb.state, CircuitState.HALF_OPEN)

        # Success resets to CLOSED
        cb.record_success()
        self.assertEqual(cb.state, CircuitState.CLOSED)
        self.assertEqual(cb.failure_count, 0)

    def test_fastapi_client_in_process_fallbacks(self):
        # Client targeting inactive port forces fallback
        client = FastAPIClient(base_url="http://localhost:59999")
        client.cb.state = CircuitState.OPEN  # Directly test fallback path

        # 1. Forecast fallback
        fc = client.predict_forecast(
            {
                "station_id": "ST_HONG_LB_01",
                "current_reading": {"dissolved_oxygen": 7.2, "ph": 7.4},
                "horizon_hours": 12,
            }
        )
        self.assertEqual(fc["_serving_mode"], "IN_PROCESS_FALLBACK")
        self.assertEqual(len(fc["trajectory"]), 12)

        # 2. Anomaly fallback
        anom = client.score_anomaly(
            {
                "station_id": "ST_HONG_LB_01",
                "reading": {"dissolved_oxygen": 1.5, "ph": 7.0, "temperature": 25.0},
            }
        )
        self.assertEqual(anom["_serving_mode"], "IN_PROCESS_FALLBACK")
        self.assertEqual(anom["severity"], "CRITICAL")

        # 3. Propagation fallback
        prop = client.simulate_propagation({"initial_bod": 20.0, "initial_do": 7.0})
        self.assertEqual(prop["_serving_mode"], "IN_PROCESS_FALLBACK")
        self.assertIn("sag_curve", prop)

        # 4. Sensor diagnostics fallback
        diag = client.diagnose_sensor(
            {
                "station_id": "ST_HONG_LB_01",
                "history_records": [{"dissolved_oxygen": 7.0}, {"dissolved_oxygen": 7.1}],
            }
        )
        self.assertEqual(diag["_serving_mode"], "IN_PROCESS_FALLBACK")
        self.assertEqual(diag["station_id"], "ST_HONG_LB_01")

        # 5. Drift status fallback
        drift = client.check_drift_status()
        self.assertEqual(drift["_serving_mode"], "IN_PROCESS_FALLBACK")
        self.assertIn("conformal_guarantee", drift)

        # 6. DLQ heal fallback
        healed = client.heal_dlq_message({"station_id": "ST_01", "dissolved_oxygen": "6.8\n"})
        self.assertEqual(healed["_serving_mode"], "IN_PROCESS_FALLBACK")
        self.assertTrue(healed["healed"])

        # 7. Health fallback
        health = client.get_health()
        self.assertEqual(health["_serving_mode"], "IN_PROCESS_FALLBACK")
        self.assertEqual(health["status"], "HEALTHY (LOCAL_FALLBACK)")

    def test_report_export_service_formats(self):
        state = self.tel_service.get_current_network_state()
        series = self.tel_service.generate_recent_series(state[0], hours=6)

        # CSV with UTF-8 BOM
        csv_bytes = ReportExportService.export_to_csv(series)
        self.assertTrue(csv_bytes.startswith(b"\xef\xbb\xbf"))
        self.assertIn(b"dissolved_oxygen", csv_bytes)

        # Excel 4-sheet workbook
        xlsx_bytes = ReportExportService.export_to_excel(
            station_meta=state[0],
            telemetry_data=series,
            forecast_data={"trajectory": [{"predicted_value": 7.1, "forecast_timestamp": "2026-11-13T12:00:00Z"}]},
        )
        self.assertGreater(len(xlsx_bytes), 1000)

        # HTML A4 print-ready report
        html_report = ReportExportService.export_to_html_report(
            station_meta=state[0],
            telemetry_data=series,
            wqi_summary={"vn_wqi_score": 88.5, "vn_wqi_label": "Tốt", "ccme_score": 90.0},
        )
        self.assertIn("BÁO CÁO KỸ THUẬT GIÁM SÁT CHẤT LƯỢNG NƯỚC MẶT", html_report)
        self.assertIn("BỘ TÀI NGUYÊN VÀ MÔI TRƯỜNG", html_report)
        self.assertIn("88.5", html_report)


class TestComponentsPackage(unittest.TestCase):
    """Test suite for SCADA atomic components."""

    def test_telemetry_trace_figure_creation(self):
        import pandas as pd

        df = pd.DataFrame(
            {
                "timestamp": ["2026-11-13T00:00:00Z", "2026-11-13T01:00:00Z"],
                "dissolved_oxygen": [7.5, 7.8],
            }
        )
        fig = create_telemetry_trace_figure(df, "dissolved_oxygen", title="DO TEST")
        self.assertEqual(len(fig.data), 1)
        self.assertEqual(fig.data[0].name, "Dissolved Oxygen")

    def test_infra_targets_and_probe_structure(self):
        self.assertEqual(len(INFRA_TARGETS), 6)
        expected_services = [
            "FastAPI :8000",
            "Grafana :30003",
            "Airflow :8085",
            "Kafka UI :8082",
            "MinIO :30001",
            "Spark :9090",
        ]
        for s in expected_services:
            self.assertIn(s, INFRA_TARGETS)

        # Probing with very short timeout
        results = probe_all_infrastructure_services(timeout_sec=0.1)
        self.assertEqual(len(results), 6)
        for name, (status, link) in results.items():
            self.assertIn(status, ["ACTIVE", "DEGRADED", "UNREACHABLE"])
            self.assertTrue(link.startswith("http"))


if __name__ == "__main__":
    unittest.main()
