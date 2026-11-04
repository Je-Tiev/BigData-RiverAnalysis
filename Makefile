# ==============================================================================
# Big Data River Quality Platform - Automation Makefile
# ==============================================================================
.PHONY: help build up down restart status logs test lint format simulator \
        streaming-lakehouse streaming-influx api dashboard quality-audit clean

# Detect Python executable
PYTHON ?= python

help:
	@echo "======================================================================"
	@echo "🌊 Big Data River Quality & MLOps Platform - Operations CLI"
	@echo "======================================================================"
	@echo "Infrastructure & Services:"
	@echo "  make build               Build Docker container images"
	@echo "  make up                  Start full Big Data & AI platform in background"
	@echo "  make down                Stop all running services"
	@echo "  make restart             Restart the entire platform"
	@echo "  make status              Display container statuses and ports"
	@echo "  make logs                Tail logs of all platform containers"
	@echo "  make clean               Stop and remove all containers and named volumes"
	@echo ""
	@echo "Development, Quality & Testing:"
	@echo "  make test                Execute full Pytest suite (Unit & Integration)"
	@echo "  make lint                Run Ruff code style and syntax checks"
	@echo "  make format              Auto-fix linting and formatting issues"
	@echo ""
	@echo "Platform Workflows & Execution:"
	@echo "  make simulator           Run IoT Hydrological Simulator (Kafka Producer)"
	@echo "  make streaming-lakehouse Run Spark Structured Streaming into Delta Lake"
	@echo "  make streaming-influx    Run Spark Structured Streaming into InfluxDB"
	@echo "  make api                 Run FastAPI Model Serving microservice locally"
	@echo "  make dashboard           Run Streamlit Operations Command Center locally"
	@echo "  make quality-audit       Run Data Quality Auditor on Lakehouse partitions"
	@echo "======================================================================"

# --- Docker Infrastructure Management ---
build:
	docker compose build

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

status:
	docker compose ps

logs:
	docker compose logs -f --tail=100

clean:
	docker compose down -v --remove-orphans

# --- Quality Assurance & Testing ---
test:
	$(PYTHON) -m pytest tests/ -v --durations=10

lint:
	$(PYTHON) -m ruff check src/ tests/ dags/ streaming/ --select=E,F,W --ignore=E501

format:
	$(PYTHON) -m ruff check src/ tests/ dags/ streaming/ --select=E,F,W --ignore=E501 --fix

# --- Data Pipelines & Microservices ---
simulator:
	$(PYTHON) src/producers/river_simulator.py

streaming-lakehouse:
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		--packages io.delta:delta-spark_2.12:3.2.0,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/jobs/spark_to_delta.py

streaming-influx:
	docker compose exec spark-master /opt/spark/bin/spark-submit \
		--master spark://spark-master:7077 \
		/opt/jobs/spark_to_influxdb.py

api:
	$(PYTHON) -m uvicorn src.serving.api:app --reload --host 0.0.0.0 --port 8000

dashboard:
	streamlit run src/dashboard/app.py --server.port 8501

quality-audit:
	$(PYTHON) -c "from src.quality.data_auditor import RiverDataQualityAuditor; auditor = RiverDataQualityAuditor(); print(auditor.audit_silver_table(station_id='station_001').to_json())"
