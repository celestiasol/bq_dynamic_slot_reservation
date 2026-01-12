import pytest, pendulum
from unittest.mock import patch, MagicMock
from datetime import datetime
from concurrent.futures import TimeoutError

from core.metrics import BigQueryJobMetricsCollector, MetricsCollectionError


@pytest.fixture
def collector(tmp_path):
    # Using a fake SQL file
    sql_file = tmp_path / "jobs_sla_metrics.sql"
    sql_file.write_text("SELECT * FROM base")  # content not used, mocked
    return BigQueryJobMetricsCollector(
        project_id="dummy_project",
        location="US",
        sql_path=str(sql_file),
        timeout_seconds=1,
    )


def make_mock_row(**overrides):
    default = {
        'count_job_submitted': 100,
        'count_job_pending': 5,
        'count_job_done': 90,
        'count_job_running': 5,
        'count_job_error': 5,
        'min_queueing_time': 0,
        'max_queueing_time': 10,
        'avg_queueing_time': 1.5,
        'stddev_queueing_time': 2.0,
        'queueing_time_p50': 1,
        'queueing_time_p96': 8,
        'queueing_time_p99': 9,
        'min_running_time': 0,
        'max_running_time': 100,
        'avg_running_time': 20,
        'stddev_running_time': 20,
    }
    default.update(overrides)
    return default


# ---------- Tests ----------

class FakeRowIterator(list):
    @property
    def total_rows(self):
        return len(self)

@patch("core.metrics.bigquery.Client")
def test_collect_success(mock_client):
    row = make_mock_row(count_job_pending=5)

    fake_rows = FakeRowIterator([row])

    mock_job = MagicMock()
    mock_job.result.return_value = fake_rows
    mock_client.return_value.query.return_value = mock_job

    collector = BigQueryJobMetricsCollector(
        project_id="test-project",
        location="US",
        sql_path="queries/jobs_sla_metrics.sql",
    )

    result = collector.collect(pendulum.now())

    assert result["count_job_pending"] == 5
    assert result["count_job_submitted"] == 100
    assert result["queueing_time_p99"] == 9

@patch("core.metrics.bigquery.Client")
def test_collect_no_rows_returns_empty(mock_client):
    fake_rows = FakeRowIterator([])

    mock_job = MagicMock()
    mock_job.result.return_value = fake_rows
    mock_client.return_value.query.return_value = mock_job

    collector = BigQueryJobMetricsCollector(
        project_id="test-project",
        location="US",
        sql_path="queries/jobs_sla_metrics.sql",
    )

    result = collector.collect(pendulum.now())

    assert result == {}

def test_collect_timeout_raises(collector):
    mock_query_job = MagicMock()
    mock_query_job.result.side_effect = TimeoutError("query exceeded timeout")

    collector.client.query = MagicMock(return_value=mock_query_job)

    with pytest.raises(
        MetricsCollectionError,
        match=r"BigQuery metrics query timed out",
    ):
        collector.collect(datetime(2026, 1, 11))

def test_collect_missing_required_fields_raises(collector):
    # Missing count_job_submitted
    mock_row = make_mock_row()
    del mock_row["count_job_submitted"]

    mock_result = MagicMock()
    mock_result.total_rows = 1
    mock_result.__iter__.return_value = [mock_row]

    mock_query_job = MagicMock()
    mock_query_job.result.return_value = mock_result

    collector.client.query = MagicMock(return_value=mock_query_job)

    with pytest.raises(
        MetricsCollectionError,
        match=r"Missing required metrics",
    ):
        collector.collect(datetime(2026, 1, 11))

