import json
import os
import time
from typing import Any, Optional
from contextlib import contextmanager

_LOG_DIR = "logs"


def _ensure_logs_dir() -> None:
    os.makedirs(_LOG_DIR, exist_ok=True)


class Logger:
    def __init__(self, call_id: str):
        self.call_id = call_id
        _ensure_logs_dir()
    
    def _get_structure(self, event:str, call_id: str, turn_id: str, time_stamp: float, model : Optional[str] = None) -> dict[str, Any]:
        return {
            "event": event,
            "call_id": call_id,
            "turn_id": turn_id,
            "model": model,
            "time_stamp": time_stamp
        }
    @contextmanager
    def span_event(self, event: str, call_id: str, turn_id: str, model: Optional[str] = None) -> None:
        timestamp_start = time.perf_counter()
        try:
          yield
        finally:
          timestamp_end = time.perf_counter()
          duration = timestamp_end - timestamp_start
          with open(f"{_LOG_DIR}/{self.call_id}.json", "a") as f:
            f.write(json.dumps(self._get_structure(event, self.call_id, turn_id, duration, model)) + "\n")
    
    def log_event(self, event: str, call_id: str, turn_id: str, model: Optional[str] = None) -> None:
        _ensure_logs_dir()
        with open(f"{_LOG_DIR}/{self.call_id}.json", "a") as f:
            f.write(json.dumps(self._get_structure(event, self.call_id, turn_id, time.perf_counter(), model)) + "\n")
