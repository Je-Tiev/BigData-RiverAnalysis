"""Resilient MLOps: Statistical Drift Monitoring, Adaptive Conformal Prediction, and Model Promotion.

Implements:
1. HydrologicalDriftMonitor: Population Stability Index (PSI) and two-sample Kolmogorov-Smirnov test.
2. AdaptiveSplitConformalPredictor: Distribution-free 95% coverage interval prediction.
3. ChampionChallengerEvaluator: Production model evaluation with empirical coverage and interval sharpness.
"""

import logging
import math
from typing import Any

import numpy as np
from scipy import stats

logger = logging.getLogger("DriftAndConformal")


class HydrologicalDriftMonitor:
    """Monitors hydrological data drift and concept drift via PSI and Kolmogorov-Smirnov tests."""

    @staticmethod
    def compute_psi(
        reference: list[float] | np.ndarray,
        current: list[float] | np.ndarray,
        num_bins: int = 10,
    ) -> float:
        """Calculates Population Stability Index (PSI) between reference and current distributions."""
        ref = np.asarray(reference, dtype=float)
        curr = np.asarray(current, dtype=float)

        if len(ref) < 2 or len(curr) < 2:
            return 0.0

        # Create quantile bin edges from reference distribution
        quantiles = np.linspace(0, 100, num_bins + 1)
        edges = np.percentile(ref, quantiles)
        edges = np.unique(edges)

        if len(edges) < 2:
            # All values in reference are identical
            return 0.0 if np.allclose(ref[0], curr) else 1.0

        edges[0] = -np.inf
        edges[-1] = np.inf

        # Calculate sample frequencies in each bin
        ref_counts, _ = np.histogram(ref, bins=edges)
        curr_counts, _ = np.histogram(curr, bins=edges)

        p = ref_counts / len(ref)
        q = curr_counts / len(curr)

        epsilon = 1e-4
        psi_sum = 0.0
        for p_i, q_i in zip(p, q, strict=False):
            # PSI = (q - p) * ln((q + eps) / (p + eps))
            psi_sum += (q_i - p_i) * math.log((q_i + epsilon) / (p_i + epsilon))

        return max(0.0, float(psi_sum))

    @staticmethod
    def compute_ks_drift(
        reference: list[float] | np.ndarray,
        current: list[float] | np.ndarray,
        alpha: float = 0.05,
    ) -> tuple[float, float, bool]:
        """Performs two-sample Kolmogorov-Smirnov test to detect distribution drift.

        Returns: (ks_statistic, p_value, drift_detected)
        """
        ref = np.asarray(reference, dtype=float)
        curr = np.asarray(current, dtype=float)

        if len(ref) < 2 or len(curr) < 2:
            return 0.0, 1.0, False

        res = stats.ks_2samp(ref, curr)
        stat = float(res.statistic)
        p_val = float(res.pvalue)
        drift_detected = bool(p_val < alpha)

        return round(stat, 4), round(p_val, 5), drift_detected

    def evaluate_station_features(
        self,
        reference_records: list[dict[str, Any]],
        current_records: list[dict[str, Any]],
        features: list[str],
    ) -> dict[str, Any]:
        """Evaluates drift metrics across multiple physicochemical features."""
        feature_reports: dict[str, Any] = {}
        drift_count = 0

        for feat in features:
            ref_vals = [float(r[feat]) for r in reference_records if feat in r and r[feat] is not None]
            curr_vals = [float(r[feat]) for r in current_records if feat in r and r[feat] is not None]

            if len(ref_vals) < 5 or len(curr_vals) < 5:
                continue

            psi_score = self.compute_psi(ref_vals, curr_vals)
            ks_stat, p_val, ks_drift = self.compute_ks_drift(ref_vals, curr_vals)

            if psi_score >= 0.25:
                psi_status = "SIGNIFICANT_DRIFT"
            elif psi_score >= 0.10:
                psi_status = "MODERATE_DRIFT"
            else:
                psi_status = "STABLE"

            is_drifting = psi_score >= 0.25 or (psi_score >= 0.10 and ks_drift)
            if is_drifting:
                drift_count += 1

            feature_reports[feat] = {
                "psi_score": round(psi_score, 4),
                "psi_status": psi_status,
                "ks_statistic": ks_stat,
                "ks_pvalue": p_val,
                "ks_drift_detected": ks_drift,
                "feature_drift": is_drifting,
            }

        if drift_count >= 2:
            overall_status = "CRITICAL"
        elif drift_count == 1:
            overall_status = "WARNING"
        else:
            overall_status = "STABLE"

        return {
            "overall_status": overall_status,
            "drifted_features_count": drift_count,
            "evaluated_features_count": len(feature_reports),
            "features": feature_reports,
        }


class AdaptiveSplitConformalPredictor:
    """Distribution-free split conformal prediction ensuring rigorous (1 - alpha) coverage guarantee."""

    def __init__(self) -> None:
        self.calibrated_q: float = 1.96
        self.alpha: float = 0.05
        self.is_calibrated: bool = False

    def calibrate(
        self,
        y_true: list[float],
        y_pred: list[float],
        alpha: float = 0.05,
        local_uncertainties: list[float] | None = None,
    ) -> None:
        """Calibrates conformal quantile score q_hat on held-out calibration set."""
        yt = np.asarray(y_true, dtype=float)
        yp = np.asarray(y_pred, dtype=float)
        n = len(yt)

        if n == 0 or len(yp) != n:
            raise ValueError("Calibration set cannot be empty and lengths must match.")

        self.alpha = float(alpha)

        if local_uncertainties is None:
            sigmas = np.ones(n, dtype=float)
        else:
            sigmas = np.asarray(local_uncertainties, dtype=float)
            sigmas = np.maximum(sigmas, 1e-4)

        # Non-conformity scores: s_i = |y_i - y_hat_i| / sigma_i
        scores = np.abs(yt - yp) / sigmas

        # Conformal quantile level: ceil((n + 1) * (1 - alpha)) / n
        level = math.ceil((n + 1) * (1.0 - self.alpha)) / n
        level = min(1.0, max(0.0, level))

        self.calibrated_q = float(np.quantile(scores, level, method="higher"))
        self.is_calibrated = True

    def predict_interval(
        self,
        point_prediction: float,
        local_uncertainty: float = 1.0,
    ) -> tuple[float, float]:
        """Constructs conformal prediction interval around point estimate."""
        y_hat = float(point_prediction)
        sigma = max(1e-4, float(local_uncertainty))
        margin = self.calibrated_q * sigma
        return round(y_hat - margin, 4), round(y_hat + margin, 4)

    @staticmethod
    def evaluate_empirical_coverage(
        y_true: list[float],
        intervals: list[tuple[float, float]],
    ) -> float:
        """Evaluates empirical coverage fraction of actual values falling within prediction intervals."""
        if not y_true or len(y_true) != len(intervals):
            return 0.0

        hits = 0
        for y, (low, high) in zip(y_true, intervals, strict=False):
            if low <= y <= high:
                hits += 1

        return round(hits / len(y_true), 4)


class ChampionChallengerEvaluator:
    """Evaluates production candidate models using predictive error, conformal coverage, and sharpness."""

    def evaluate_models(
        self,
        y_true: list[float],
        y_pred_champ: list[float],
        y_pred_chall: list[float],
        intervals_champ: list[tuple[float, float]],
        intervals_chall: list[tuple[float, float]],
    ) -> dict[str, Any]:
        """Evaluates champion vs challenger models and recommends promotion or retention."""
        yt = np.asarray(y_true, dtype=float)
        yp_champ = np.asarray(y_pred_champ, dtype=float)
        yp_chall = np.asarray(y_pred_chall, dtype=float)
        n = len(yt)

        if n == 0:
            raise ValueError("Evaluation series cannot be empty.")

        # 1. Point accuracy metrics
        rmse_champ = float(np.sqrt(np.mean((yt - yp_champ) ** 2)))
        rmse_chall = float(np.sqrt(np.mean((yt - yp_chall) ** 2)))

        mae_champ = float(np.mean(np.abs(yt - yp_champ)))
        mae_chall = float(np.mean(np.abs(yt - yp_chall)))

        # 2. Conformal empirical coverage
        cov_champ = AdaptiveSplitConformalPredictor.evaluate_empirical_coverage(y_true, intervals_champ)
        cov_chall = AdaptiveSplitConformalPredictor.evaluate_empirical_coverage(y_true, intervals_chall)

        # 3. Mean Prediction Interval Width (MPIW - Sharpness)
        mpiw_champ = float(np.mean([high - low for low, high in intervals_champ]))
        mpiw_chall = float(np.mean([high - low for low, high in intervals_chall]))

        # Relative RMSE improvement: (RMSE_champ - RMSE_chall) / RMSE_champ
        rmse_diff_pct = (rmse_champ - rmse_chall) / max(1e-6, rmse_champ)

        # Promotion logic: >= 5% RMSE improvement and challenger coverage >= 93% (0.93)
        meets_accuracy = rmse_diff_pct >= 0.05
        meets_coverage = cov_chall >= 0.93

        should_promote = meets_accuracy and meets_coverage

        if should_promote:
            decision = "PROMOTE"
            narrative = (
                f"Challenger model outperforms Champion with {rmse_diff_pct * 100:.1f}% lower RMSE "
                f"({rmse_chall:.3f} vs {rmse_champ:.3f}) and maintains valid coverage of {cov_chall * 100:.1f}% >= 93%."
            )
        else:
            decision = "KEEP_CHAMPION"
            reasons = []
            if not meets_accuracy:
                reasons.append(f"RMSE improvement {rmse_diff_pct * 100:.1f}% below required 5.0% threshold")
            if not meets_coverage:
                reasons.append(f"Empirical coverage {cov_chall * 100:.1f}% below required 93.0% threshold")
            narrative = f"Champion model retained: {'; '.join(reasons)}."

        return {
            "decision": decision,
            "narrative": narrative,
            "metrics": {
                "rmse_champion": round(rmse_champ, 4),
                "rmse_challenger": round(rmse_chall, 4),
                "rmse_improvement_pct": round(rmse_diff_pct * 100, 2),
                "mae_champion": round(mae_champ, 4),
                "mae_challenger": round(mae_chall, 4),
                "coverage_champion": cov_champ,
                "coverage_challenger": cov_chall,
                "mpiw_champion": round(mpiw_champ, 3),
                "mpiw_challenger": round(mpiw_chall, 3),
            },
        }
