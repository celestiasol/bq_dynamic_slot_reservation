import logging
import pendulum
from typing import Dict, Any

from core.metrics import BigQueryJobMetricsCollector, MetricsCollectionError
from core.sla_policy import SLAPolicy, SLAEvaluationResult
from core.reservation import BigQuerySlotReservation, assert_max_slot_value


class ReservationManager:
    """Wrapper around BigQuerySlotReservation with utility methods."""

    def __init__(self, project_id: str, reservation_id: str, location: str):
        self.reservation_client = BigQuerySlotReservation(
            project_id=project_id,
            reservation_id=reservation_id,
            zone=location
        )

    def get_current_slots(self) -> int:
        return self.reservation_client.get()["autoscale_max_slots"]

    def add_slots(self, increment: int) -> None:
        current = self.get_current_slots()
        new_value = current + increment
        self.set_slots(new_value)

    def set_slots(self, value: int) -> None:
        value = assert_max_slot_value(value)
        response = self.reservation_client.update(
            max_autoscaling_slot=value,
            ignore_idle_slots=True
        )
        logging.info(f"Updated reservation: {response['content']}")


class DecisionEngine:
    """Core decision engine for slot adjustment based on SLA & metrics."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        metadata = config["metadata"]

        self.slot_profiles = config.get("reservation_slot_profiles", {})
        self.time_mapping = config.get("reservation_time_mapping", {})
        self.check_interval_minutes = config.get("check_interval_minutes", 5)

        self.collector = BigQueryJobMetricsCollector(
            project_id=metadata["project_id"],
            location=metadata.get("location", "US"),
            sql_path=config.get("sql_path", "queries/jobs_sla_metrics.sql"),
        )

        self.sla_policy = SLAPolicy(config.get("sla_thresholds", {}))
        self.reservation_mgr = ReservationManager(
            project_id=metadata["project_id"],
            reservation_id=metadata["reservation_id"],
            location=metadata.get("location", "asia-southeast2")
        )
        self.default_adjustment = config.get("default_adjustment_slots", 50)

    def _normalize_execution_time(self, execution_time) -> pendulum.DateTime:
        if isinstance(execution_time, pendulum.DateTime):
            return execution_time

        if isinstance(execution_time, str):
            return pendulum.parse(execution_time)

        # datetime.datetime support
        return pendulum.instance(execution_time)

    def get_slot_config_for_time(self, dt: pendulum.DateTime) -> Dict[str, int]:
        """Resolve slot config (min/max/increment) for a given datetime."""
        day = dt.day_of_week
        hour = dt.hour
        minute = (dt.minute // 30) * 30  # round down to nearest 30-min interval

        day_mapping = self.time_mapping.get(str(day), {})
        hour_mapping = day_mapping.get(str(hour), {})
        minute_config = hour_mapping.get(str(minute))

        if isinstance(minute_config, str):
            # resolve from slot profile
            return self.slot_profiles[minute_config]
        elif isinstance(minute_config, dict):
            return minute_config
        else:
            # fallback to default
            return {"min": 1500, "max": 4000, "increment": 100}

    def run(self, execution_time) -> None:
        """Main decision logic: collect metrics, evaluate SLA, adjust slots."""
        execution_time = self._normalize_execution_time(execution_time)
        monitoring_time = execution_time - pendulum.duration(minutes=5)
        slot_config = self.get_slot_config_for_time(execution_time)

        # Step 1: Collect metrics
        try:
            metrics = self.collector.collect(monitoring_time)
            logging.info(f"Collected metrics at {monitoring_time}: {metrics}")
        except MetricsCollectionError as e:
            logging.error(f"Metrics collection failed: {e}")
            # SLA cannot be evaluated; consider increasing slots defensively
            self.reservation_mgr.add_slots(self.default_adjustment)
            return

        # Step 2: Evaluate SLA
        result: SLAEvaluationResult = self.sla_policy.evaluate(metrics)
        current_slots = self.reservation_mgr.get_current_slots()

        if result.healthy:
            logging.info("SLA healthy — no adjustment needed")
            # optionally enforce min slots at the start of a 30-min window
            if execution_time.minute % 30 == 0 and current_slots != slot_config["min"]:
                self.reservation_mgr.set_slots(slot_config["min"])
            return

        # Step 3: SLA breach detected → increase slots
        logging.warning(f"SLA breach detected: {result.violations}")
        increment = self.default_adjustment
        proposed_slots = min(current_slots + increment, slot_config["max"])
        self.reservation_mgr.set_slots(proposed_slots)
        logging.info(f"Slots updated to {proposed_slots} due to SLA breach")

