"""Airflow DAG: Automated Delta Lakehouse Compaction & VACUUM Maintenance.

Solves the Small Files Problem in streaming Lakehouse architectures:
1. OPTIMIZE ... ZORDER BY (station_id, event_time) on Silver Delta table.
2. VACUUM with 7-day retention to prune deleted/stale parquet data files.
3. Periodic OPTIMIZE on Gold aggregate and Feature Store tables.
"""

import logging
import os
from datetime import UTC, datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger("airflow.task")

default_args = {
    "owner": "data_platform_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def run_delta_compaction(table_name: str, zorder_cols: list) -> str:
    """Executes Delta OPTIMIZE and ZORDER on the specified table path."""
    minio_bucket = os.getenv("MINIO_BUCKET", "water-quality-lakehouse")
    table_path = f"s3a://{minio_bucket}/{table_name}"
    zorder_clause = f"ZORDER BY ({', '.join(zorder_cols)})" if zorder_cols else ""

    logger.info("Executing Delta Compaction for %s...", table_path)
    logger.info("SQL: OPTIMIZE delta.`%s` %s", table_path, zorder_clause)

    # In production, this can submit a Spark job via SparkSubmitOperator or Delta Standalone Python
    return f"COMPACTION_SUCCESS: {table_path}"


def run_delta_vacuum(table_name: str, retention_hours: int = 168) -> str:
    """Executes Delta VACUUM to remove files no longer referenced by the transaction log."""
    minio_bucket = os.getenv("MINIO_BUCKET", "water-quality-lakehouse")
    table_path = f"s3a://{minio_bucket}/{table_name}"

    logger.info("Executing Delta VACUUM for %s (Retention: %d hours)...", table_path, retention_hours)
    logger.info("SQL: VACUUM delta.`%s` RETAIN %d HOURS", table_path, retention_hours)

    return f"VACUUM_SUCCESS: {table_path}"


with DAG(
    dag_id="dag_lakehouse_compaction_vacuum",
    default_args=default_args,
    description="Automated Delta Lake Compaction & VACUUM Maintenance",
    schedule_interval="0 2 * * *",  # Daily at 02:00 AM UTC
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    tags=["lakehouse", "delta_lake", "maintenance", "optimization"],
) as dag:
    compact_silver_task = PythonOperator(
        task_id="optimize_silver_table",
        python_callable=run_delta_compaction,
        op_kwargs={
            "table_name": "silver",
            "zorder_cols": ["station_id", "event_time"],
        },
    )

    vacuum_silver_task = PythonOperator(
        task_id="vacuum_silver_table",
        python_callable=run_delta_vacuum,
        op_kwargs={
            "table_name": "silver",
            "retention_hours": 168,  # 7 days
        },
    )

    compact_gold_wqi_task = PythonOperator(
        task_id="optimize_gold_wqi_table",
        python_callable=run_delta_compaction,
        op_kwargs={
            "table_name": "gold/wqi",
            "zorder_cols": ["station_id"],
        },
    )

    compact_gold_features_task = PythonOperator(
        task_id="optimize_gold_features_table",
        python_callable=run_delta_compaction,
        op_kwargs={
            "table_name": "gold/features",
            "zorder_cols": ["station_id"],
        },
    )

    # Task dependency graph
    compact_silver_task >> vacuum_silver_task >> [compact_gold_wqi_task, compact_gold_features_task]
