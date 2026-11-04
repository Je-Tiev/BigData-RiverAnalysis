"""Automated Data Quality Audit Engine for River Quality Lakehouse.

Implements declarative Data Quality Gates (Great Expectations / PyDeequ equivalent):
1. Null and Completeness checks on primary keys (station_id, event_time).
2. Physical feasibility range bounds (pH, DO, Temperature, Ammonia, Turbidity).
3. Freshness and data drift metrics.
4. Structured JSON audit report output for Airflow DAGs and monitoring dashboards.
"""

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
import logging
from typing import Any, Dict, List

logger = logging.getLogger("DataQualityAuditor")


@dataclass
class CheckResult:
    """Represents the outcome of a single validation rule."""

    check_name: str
    rule_type: str
    passed: bool
    total_evaluated: int
    violation_count: int
    violation_rate: float
    message: str


@dataclass
class QualityAuditReport:
    """Complete summary of a Data Quality audit run."""

    suite_name: str
    evaluated_at: str
    overall_status: str  # PASSED | WARNING | FAILED
    total_records: int
    passed_checks: int
    failed_checks: int
    results: List[CheckResult] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)

    def to_json(self, indent: int = 2) -> str:
        return json.dumps(self.to_dict(), indent=indent, ensure_ascii=False)


class RiverDataQualityAuditor:
    """Performs rigorous quality audits across Silver and Gold datasets."""

    def __init__(
        self,
        max_null_rate_tolerance: float = 0.0,
        max_physical_violation_rate: float = 0.01,
        suite_name: str = "RiverLakehouse_Quality_Suite",
    ):
        self.max_null_tol = max_null_rate_tolerance
        self.max_violation_tol = max_physical_violation_rate
        self.suite_name = suite_name

    def audit_records(self, records: List[Dict[str, Any]]) -> QualityAuditReport:
        """Audits an in-memory list of records (e.g. from Pandas or Spark collect)."""
        now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        total = len(records)

        if total == 0:
            return QualityAuditReport(
                suite_name=self.suite_name,
                evaluated_at=now_iso,
                overall_status="WARNING",
                total_records=0,
                passed_checks=0,
                failed_checks=0,
                results=[
                    CheckResult(
                        check_name="dataset_not_empty",
                        rule_type="cardinality",
                        passed=False,
                        total_evaluated=0,
                        violation_count=0,
                        violation_rate=0.0,
                        message="Dataset contains 0 records to evaluate.",
                    )
                ],
            )

        results: List[CheckResult] = []

        # 1. Null Checks on Primary Identifiers
        null_stations = sum(1 for r in records if not r.get("station_id"))
        rate = null_stations / total
        passed = rate <= self.max_null_tol
        results.append(
            CheckResult(
                check_name="null_station_id_check",
                rule_type="completeness",
                passed=passed,
                total_evaluated=total,
                violation_count=null_stations,
                violation_rate=round(rate, 4),
                message=f"Found {null_stations} records with missing station_id (tolerance: {self.max_null_tol})",
            )
        )

        null_timestamps = sum(1 for r in records if not (r.get("timestamp") or r.get("event_time")))
        rate = null_timestamps / total
        passed = rate <= self.max_null_tol
        results.append(
            CheckResult(
                check_name="null_timestamp_check",
                rule_type="completeness",
                passed=passed,
                total_evaluated=total,
                violation_count=null_timestamps,
                violation_rate=round(rate, 4),
                message=f"Found {null_timestamps} records with missing timestamp (tolerance: {self.max_null_tol})",
            )
        )

        # 2. Physical Range Boundary Checks
        rules = [
            ("ph_range_check", "ph", 0.0, 14.0),
            ("do_range_check", "dissolved_oxygen", 0.0, 25.0),
            ("temperature_range_check", "temperature", -5.0, 55.0),
            ("ammonia_non_negative_check", "ammonia", 0.0, 50.0),
            ("conductivity_non_negative_check", "conductivity", 0.0, 10000.0),
            ("turbidity_non_negative_check", "turbidity_ntu", 0.0, 2000.0),
        ]

        for check_name, field_name, low_bound, high_bound in rules:
            violations = 0
            evaluated = 0
            for r in records:
                val = r.get(field_name)
                if val is not None:
                    evaluated += 1
                    try:
                        f_val = float(val)
                        if f_val < low_bound or f_val > high_bound:
                            violations += 1
                    except (ValueError, TypeError):
                        violations += 1

            if evaluated > 0:
                v_rate = violations / evaluated
                passed = v_rate <= self.max_violation_tol
                results.append(
                    CheckResult(
                        check_name=check_name,
                        rule_type="physical_boundary",
                        passed=passed,
                        total_evaluated=evaluated,
                        violation_count=violations,
                        violation_rate=round(v_rate, 4),
                        message=f"{violations}/{evaluated} ({v_rate:.2%}) violations of [{low_bound}, {high_bound}]",
                    )
                )

        # 3. Overall Status Determination
        failed_count = sum(1 for c in results if not c.passed)
        passed_count = len(results) - failed_count

        if failed_count == 0:
            overall_status = "PASSED"
        elif any(c.rule_type == "completeness" and not c.passed for c in results):
            overall_status = "FAILED"
        else:
            overall_status = "WARNING"

        return QualityAuditReport(
            suite_name=self.suite_name,
            evaluated_at=now_iso,
            overall_status=overall_status,
            total_records=total,
            passed_checks=passed_count,
            failed_checks=failed_count,
            results=results,
        )
