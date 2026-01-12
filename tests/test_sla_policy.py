import pytest

from core.sla_policy import SLAPolicy, SLAEvaluationResult


@pytest.fixture
def default_policy():
    return SLAPolicy(
        thresholds={
            "pending_job_pct": 15.0,
            "queueing_time_p99": 180,
            "max_running_time": 420,
            "error_job_pct": 1.0,
        }
    )


def test_sla_healthy(default_policy):
    metrics = {
        "count_job_submitted": 100,
        "count_job_pending": 1,
        "count_job_error": 0,
        "queueing_time_p99": 10,
        "max_running_time": None,
    }

    result = default_policy.evaluate(metrics)

    assert result.healthy is True
    assert result.violations == []


def test_pending_job_pct_breach(default_policy):
    metrics = {
        "count_job_submitted": 100,
        "count_job_pending": 20,
        "count_job_error": 0,
        "queueing_time_p99": 10,
        "max_running_time": 50,
    }

    result = default_policy.evaluate(metrics)

    assert result.healthy is False
    assert len(result.violations) == 1
    assert result.violations[0].metric == "pending_job_pct"


def test_queueing_time_p99_breach(default_policy):
    metrics = {
        "count_job_submitted": 100,
        "count_job_pending": 1,
        "count_job_error": 0,
        "queueing_time_p99": 240,
        "max_running_time": 50,
    }

    result = default_policy.evaluate(metrics)

    assert result.healthy is False
    assert result.violations[0].metric == "queueing_time_p99"


def test_max_running_time_breach(default_policy):
    metrics = {
        "count_job_submitted": 5,
        "count_job_pending": 0,
        "count_job_error": 0,
        "queueing_time_p99": 1,
        "max_running_time": 520,
    }

    result = default_policy.evaluate(metrics)

    assert result.healthy is False
    assert result.violations[0].metric == "max_running_time"


def test_multiple_breaches(default_policy):
    metrics = {
        "count_job_submitted": 100,
        "count_job_pending": 20,
        "count_job_error": 5,
        "queueing_time_p99": 240,
        "max_running_time": 520,
    }

    result = default_policy.evaluate(metrics)

    assert result.healthy is False
    assert {v.metric for v in result.violations} == {
        "pending_job_pct",
        "queueing_time_p99",
        "max_running_time",
        "error_job_pct",
    }


def test_zero_jobs_submitted_is_healthy(default_policy):
    metrics = {
        "count_job_submitted": 0,
        "count_job_pending": 10,
        "count_job_error": 0,
        "queueing_time_p99": 0,
        "max_running_time": 0,
    }

    result = default_policy.evaluate(metrics)

    assert result.healthy is True
    assert result.violations == []


def test_empty_metrics_is_healthy(default_policy):
    result = default_policy.evaluate({})

    assert result.healthy is True
    assert result.violations == []


def test_missing_required_metric_marks_unhealthy(default_policy, caplog):
    metrics = {
        # missing count_job_submitted
        "count_job_pending": 10,
        "count_job_error": 0,
        "queueing_time_p99": 0,
        "max_running_time": 0,
    }

    result = default_policy.evaluate(metrics)

    assert result.healthy is False
