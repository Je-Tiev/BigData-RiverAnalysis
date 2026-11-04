"""Unit and Integration Tests for Visualization & Observability (Phase 4).

Validates Dashboard helper functions, GIS network state generators, Prometheus
scrape configuration, and Grafana infrastructure monitoring dashboards.
"""

import json
import os
import sys
import unittest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from src.contracts.river_schema import REFERENCE_STATIONS
from src.dashboard.app import generate_recent_series, get_current_network_state


class TestDashboardComponents(unittest.TestCase):
    """Test suite for Streamlit Dashboard data and UI helper functions."""

    def test_network_state_contains_all_10_stations(self):
        state = get_current_network_state()
        self.assertEqual(len(state), 10)
        station_ids = [s["station_id"] for s in state]
        for ref in REFERENCE_STATIONS:
            self.assertIn(ref["station_id"], station_ids)

    def test_network_state_record_fields(self):
        state = get_current_network_state()
        first = state[0]
        required_keys = [
            "station_id", "station_name", "river_name", "basin_name",
            "latitude", "longitude", "temperature", "ph", "dissolved_oxygen",
            "ccme_score", "quality_label", "is_anomaly",
        ]
        for key in required_keys:
            self.assertIn(key, first, f"Missing required key in network state: {key}")

    def test_generate_recent_series_hourly_continuity(self):
        st_meta = REFERENCE_STATIONS[0]
        series = generate_recent_series(st_meta, hours=24)
        self.assertEqual(len(series), 25)  # 24 steps + initial point
        for pt in series:
            self.assertIn("dissolved_oxygen", pt)
            self.assertIn("timestamp", pt)
            self.assertGreater(pt["dissolved_oxygen"], 0.0)


class TestObservabilityConfigs(unittest.TestCase):
    """Test suite for Prometheus and Grafana configuration files."""

    def setUp(self):
        self.repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    def test_prometheus_scrape_config_exists_and_contains_jobs(self):
        prom_path = os.path.join(self.repo_root, "monitoring", "prometheus", "prometheus.yml")
        self.assertTrue(os.path.exists(prom_path), f"Prometheus config missing: {prom_path}")

        with open(prom_path, "r", encoding="utf-8") as f:
            content = f.read()

        # Check for core scrape targets
        self.assertIn("job_name: \"spark_cluster\"", content)
        self.assertIn("job_name: \"model_serving_api\"", content)
        self.assertIn("job_name: \"influxdb\"", content)

    def test_grafana_datasource_prometheus_exists(self):
        ds_path = os.path.join(self.repo_root, "grafana", "provisioning", "datasources", "prometheus.yml")
        self.assertTrue(os.path.exists(ds_path), f"Grafana prometheus datasource missing: {ds_path}")

        with open(ds_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("type: prometheus", content)
        self.assertIn("url: http://prometheus:9090", content)

    def test_bigdata_infrastructure_dashboard_json_validity(self):
        dash_path = os.path.join(self.repo_root, "grafana", "dashboards", "bigdata_infrastructure_dashboard.json")
        self.assertTrue(os.path.exists(dash_path), f"Dashboard JSON missing: {dash_path}")

        with open(dash_path, "r", encoding="utf-8") as f:
            dash = json.load(f)

        self.assertEqual(dash["title"], "Big Data Platform & Pipeline Health")
        self.assertIn("panels", dash)
        panel_titles = [p.get("title") for p in dash["panels"]]
        self.assertIn("Kafka Consumer Group Lag (Messages)", panel_titles)
        self.assertIn("Spark Streaming Batch Duration (ms)", panel_titles)
        self.assertIn("Spark Executor JVM Heap Memory Usage", panel_titles)

    def test_docker_compose_contains_prometheus_and_airflow(self):
        compose_path = os.path.join(self.repo_root, "docker-compose.yml")
        with open(compose_path, "r", encoding="utf-8") as f:
            content = f.read()

        self.assertIn("prometheus:", content)
        self.assertIn("9095:9090", content)
        self.assertIn("airflow-webserver:", content)
        self.assertIn("spark-master:", content)


if __name__ == "__main__":
    unittest.main()
