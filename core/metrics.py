from datetime import datetime
from typing import Dict, Any
from google.cloud import bigquery
from concurrent.futures import TimeoutError
import logging
import pathlib


class MetricsCollectionError(Exception):
    pass


class BigQueryJobMetricsCollector:
    def __init__(
        self,
        project_id: str,
        location: str,
        sql_path: str,
        timeout_seconds: int = 60,
    ):
        self.client = bigquery.Client(project=project_id)
        self.location = location
        self.timeout_seconds = timeout_seconds

        sql_file = pathlib.Path(sql_path)
        self.query_template = sql_file.read_text()

    def collect(self, since: datetime) -> Dict[str, Any]:
        """
        Collect aggregated BigQuery job metrics since the given timestamp.
        """

        query = self.query_template.replace("{{ location }}", self.location)

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter(
                    "since_ts", "TIMESTAMP", since
                )
            ]
        )

        try:
            query_job = self.client.query(
                query,
                job_config=job_config,
            )

            result = query_job.result(timeout=self.timeout_seconds)

            if result.total_rows == 0:
                return {}

            row = list(result)[0]
            metrics = dict(row)

            self._validate_metrics(metrics)

            return metrics

        except TimeoutError:
            raise MetricsCollectionError("BigQuery metrics query timed out")

        except Exception as exc:
            logging.exception("Failed to collect BigQuery job metrics")
            raise MetricsCollectionError(str(exc)) from exc

    @staticmethod
    def _validate_metrics(metrics: Dict[str, Any]) -> None:
        required_fields = {
            "count_job_submitted",
            "count_job_pending",
            "count_job_error",
            "queueing_time_p99",
            "max_running_time",
        }

        missing = required_fields - metrics.keys()
        if missing:
            raise MetricsCollectionError(
                f"Missing required metrics: {missing}"
            )

