import logging
from dataclasses import dataclass
from typing import Dict, List


@dataclass
class SLAViolation:
    metric: str
    value: float
    threshold: float
    message: str


@dataclass
class SLAEvaluationResult:
    healthy: bool
    violations: List[SLAViolation]


class SLAPolicy:
    """
    Applies SLA rules to aggregated BigQuery job metrics.
    This class is intentionally agnostic of how metrics are collected.
    """

    def __init__(self, thresholds: Dict[str, float]):
        self.thresholds = thresholds

    def evaluate(self, metrics: Dict) -> SLAEvaluationResult:
        """
        Expected metrics keys:
          - count_job_submitted
          - count_job_pending
          - count_job_error
          - queueing_time_p99
          - max_running_time
        """

        if not metrics:
            # No workload = healthy system
            return SLAEvaluationResult(healthy=True, violations=[])

        violations: List[SLAViolation] = []

        try:
            submitted = metrics["count_job_submitted"]

            if submitted == 0:
                return SLAEvaluationResult(healthy=True, violations=[])

            pending_pct = metrics["count_job_pending"] / submitted * 100
            error_pct = metrics["count_job_error"] / submitted * 100
            queueing_p99 = metrics["queueing_time_p99"]
            max_runtime = metrics.get("max_running_time", 0) or 0

            if pending_pct > self.thresholds["pending_job_pct"]:
                violations.append(
                    SLAViolation(
                        metric="pending_job_pct",
                        value=pending_pct,
                        threshold=self.thresholds["pending_job_pct"],
                        message=f"Pending job percentage {pending_pct:.2f}% exceeds threshold"
                    )
                )

            if queueing_p99 > self.thresholds["queueing_time_p99"]:
                violations.append(
                    SLAViolation(
                        metric="queueing_time_p99",
                        value=queueing_p99,
                        threshold=self.thresholds["queueing_time_p99"],
                        message=f"Queueing P99 {queueing_p99:.2f}s exceeds threshold"
                    )
                )

            if max_runtime > self.thresholds["max_running_time"]:
                violations.append(
                    SLAViolation(
                        metric="max_running_time",
                        value=max_runtime,
                        threshold=self.thresholds["max_running_time"],
                        message=f"Max runtime {max_runtime:.2f}s exceeds threshold"
                    )
                )

            if "error_job_pct" in self.thresholds and error_pct > self.thresholds["error_job_pct"]:
                violations.append(
                    SLAViolation(
                        metric="error_job_pct",
                        value=error_pct,
                        threshold=self.thresholds["error_job_pct"],
                        message=f"Error rate {error_pct:.2f}% exceeds threshold"
                    )
                )

            return SLAEvaluationResult(
                healthy=len(violations) == 0,
                violations=violations
            )

        except KeyError as e:
            logging.error(f"Missing required SLA metric: {e}")
            return SLAEvaluationResult(healthy=False, violations=[])

