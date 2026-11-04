"""MLflow Tracking & Model Registry Integration Module.

Manages MLOps lifecycle:
1. Logs hyperparameters, metrics (RMSE, MAE, R2), and model artifacts to MLflow.
2. Supports local fallback metadata tracking for air-gapped / offline testing.
3. Manages Model Registry version transitions (Staging -> Production).
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
import os
import shutil
from typing import Any, Dict, Optional

logger = logging.getLogger("MLflowTracker")


@dataclass
class ExperimentRunMetadata:
    """Stores full telemetry and artifacts manifest for a single training run."""

    run_id: str
    experiment_name: str
    model_type: str
    status: str  # COMPLETED | FAILED
    parameters: Dict[str, Any]
    metrics: Dict[str, float]
    artifact_path: str
    registered_model_version: Optional[str] = None
    stage: str = "None"  # None | Staging | Production | Archived
    created_at: str = field(
        default_factory=lambda: datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    )

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class RiverMLflowTracker:
    """Wrapper for MLflow Tracking Server with local metadata failover."""

    def __init__(
        self,
        experiment_name: str = "River_WaterQuality_ML",
        tracking_uri: Optional[str] = None,
        local_store_dir: Optional[str] = None,
    ):
        self.experiment_name = experiment_name
        self.tracking_uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
        self.local_store_dir = local_store_dir or os.path.join(
            os.path.dirname(__file__), ".mlflow_local"
        )
        os.makedirs(self.local_store_dir, exist_ok=True)
        self._mlflow = None
        self._init_mlflow()

    def _init_mlflow(self):
        """Attempts to connect to MLflow client."""
        try:
            import mlflow
            self._mlflow = mlflow
            self._mlflow.set_tracking_uri(self.tracking_uri)
            self._mlflow.set_experiment(self.experiment_name)
            logger.info("Connected to MLflow Tracking Server at %s", self.tracking_uri)
        except Exception as e:
            logger.info("MLflow remote server deferred, using local metadata store: %s", e)

    def log_training_run(
        self,
        model_name: str,
        parameters: Dict[str, Any],
        metrics: Dict[str, float],
        model_artifact_path: Optional[str] = None,
    ) -> ExperimentRunMetadata:
        """Logs a model training run with parameters, metrics, and saves metadata."""
        import uuid
        run_id = f"run_{uuid.uuid4().hex[:12]}"
        run_dir = os.path.join(self.local_store_dir, run_id)
        os.makedirs(run_dir, exist_ok=True)

        artifact_dest = run_dir
        if model_artifact_path and os.path.exists(model_artifact_path):
            artifact_dest = os.path.join(run_dir, os.path.basename(model_artifact_path))
            if os.path.isfile(model_artifact_path):
                shutil.copy2(model_artifact_path, artifact_dest)

        meta = ExperimentRunMetadata(
            run_id=run_id,
            experiment_name=self.experiment_name,
            model_type=model_name,
            status="COMPLETED",
            parameters=parameters,
            metrics=metrics,
            artifact_path=artifact_dest,
            stage="Staging",
        )

        # Save metadata record locally
        meta_file = os.path.join(run_dir, "metadata.json")
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(meta.to_dict(), f, indent=2, ensure_ascii=False)

        # Log to remote MLflow if active
        if self._mlflow is not None:
            try:
                with self._mlflow.start_run(run_name=f"{model_name}_{run_id}") as active_run:
                    self._mlflow.log_params(parameters)
                    self._mlflow.log_metrics(metrics)
                    if model_artifact_path and os.path.exists(model_artifact_path):
                        self._mlflow.log_artifact(model_artifact_path)
                    logger.info("Logged run %s to remote MLflow.", active_run.info.run_id)
            except Exception as e:
                logger.warning("Could not log to remote MLflow: %s", e)

        logger.info("Run %s recorded successfully (Model: %s, Metrics: %s)", run_id, model_name, metrics)
        return meta

    def promote_to_production(self, run_id: str) -> bool:
        """Promotes a trained model run to Production stage."""
        meta_file = os.path.join(self.local_store_dir, run_id, "metadata.json")
        if not os.path.exists(meta_file):
            logger.error("Run ID %s not found in local store.", run_id)
            return False

        with open(meta_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["stage"] = "Production"
        with open(meta_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logger.info("Promoted Model Run %s to Production stage.", run_id)
        return True
