import json
import logging
import pendulum
from pathlib import Path
from typing import Any, Dict

from core.decision_engine import DecisionEngine

class SlotController:
    """Wrapper to manage slot adjustments using the DecisionEngine."""

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.engine = DecisionEngine(config)

    @classmethod
    def from_file(cls, path: str) -> "SlotController":
        """Load config from JSON file."""
        path_obj = Path(path)
        if not path_obj.exists():
            raise FileNotFoundError(f"Config file not found: {path}")

        with path_obj.open("r") as f:
            config = json.load(f)
        return cls(config)

    def run(self, execution_time: Any = None) -> None:
        """Run decision engine at the specified execution time."""
        if execution_time is None:
            execution_time = pendulum.now("Asia/Jakarta")

        if not isinstance(execution_time, pendulum.DateTime):
            # allow datetime.datetime objects
            execution_time = pendulum.instance(execution_time, tz="Asia/Jakarta")

        logging.info(f"Running slot controller at {execution_time}")
        self.engine.run(execution_time)
