import pytest
from unittest.mock import MagicMock, patch
from core.decision_engine import DecisionEngine

@pytest.fixture
def mock_config():
    return {
        "metadata": {
            "project_id": "test-project",
            "reservation_id": "res-123",
            "location": "asia-southeast2"
        },
        "check_interval_minutes": 5,
        "reservation_slot_profiles": {
            "low": { "min": 1500, "max": 2000, "increment": 100 },
            "medium": { "min": 2500, "max": 3000, "increment": 100 },
            "high": { "min": 3500, "max": 4000, "increment": 100 }
        },
        "reservation_time_mapping": {
            0: {  # Monday
                9: {"0": "high", "30": "high"},
                10: {"0": "low", "30": "low"}
            }
        },
        "default_adjustment_slots": 50
    }

@patch("core.decision_engine.ReservationManager")
def test_engine_increases_slot_on_sla_breach(mock_reservation_mgr, mock_config):
    # Mock reservation manager instance
    mock_instance = MagicMock()
    mock_instance.get_current_slots.return_value = 1000
    mock_reservation_mgr.return_value = mock_instance

    engine = DecisionEngine(mock_config)

    engine.collector.collect = MagicMock(return_value={"count_job_pending": 10})
    engine.sla_policy.evaluate = MagicMock(
        return_value=MagicMock(healthy=False, violations=["pending too high"])
    )

    engine.run("2026-01-11T09:05:00") # at minute 5 SLA checking

    mock_instance.set_slots.assert_called_once()

@patch("core.decision_engine.ReservationManager")
def test_engine_no_change_on_healthy_sla(mock_reservation_mgr, mock_config):
    mock_instance = MagicMock()
    mock_instance.get_current_slots.return_value = 1000
    mock_reservation_mgr.return_value = mock_instance

    engine = DecisionEngine(mock_config)

    engine.collector.collect = MagicMock(return_value={"count_job_pending": 1})
    engine.sla_policy.evaluate = MagicMock(
        return_value=MagicMock(healthy=True)
    )

    engine.run("2026-01-11T09:05:00") # at minute 5 SLA checking

    mock_instance.set_slots.assert_not_called()
    mock_instance.add_slots.assert_not_called()


# Enforce baseline slot capacity at window boundaries
@patch("core.decision_engine.ReservationManager")
def test_engine_sets_min_slots_on_healthy_sla_window_boundary(mock_reservation_mgr, mock_config):
    mock_instance = MagicMock()
    mock_instance.get_current_slots.return_value = 1000
    mock_reservation_mgr.return_value = mock_instance

    engine = DecisionEngine(mock_config)

    engine.collector.collect = MagicMock(return_value={"count_job_pending": 1})
    engine.sla_policy.evaluate = MagicMock(
        return_value=MagicMock(healthy=True)
    )

    engine.run("2026-01-11T08:00:00")  # minute == 0

    mock_instance.set_slots.assert_called_once()

@patch("core.decision_engine.ReservationManager")
def test_engine_no_change_on_healthy_sla_non_boundary(mock_reservation_mgr, mock_config):
    mock_instance = MagicMock()
    mock_instance.get_current_slots.return_value = 1000
    mock_reservation_mgr.return_value = mock_instance

    engine = DecisionEngine(mock_config)

    engine.collector.collect = MagicMock(return_value={"count_job_pending": 1})
    engine.sla_policy.evaluate = MagicMock(
        return_value=MagicMock(healthy=True)
    )

    engine.run("2026-01-11T08:10:00")  # minute != 0 or 30

    mock_instance.set_slots.assert_not_called()
    mock_instance.add_slots.assert_not_called()
