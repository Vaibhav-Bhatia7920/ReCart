import json
import os
import time



class Logger:
    def __init__(self, call_id: str):
        self.call_id = call_id
    
    def _get_structure(self, event:str, call_id: str, turn_id: str, time_duration: float, model : Optional[str] = None) -> dict[str, Any]:
        return {
            "event": event,
            "call_id": call_id,
            "turn_id": turn_id,
            "model": model,
            "timestamp": time.perf_counter()
        }

    def span_event(self, event: str, call_id: str, turn_id: str, model: Optional[str] = None) -> None:
        os.makedirs("logs", exist_ok=True)
        timestamp_start = time.perf_counter()
        try:
          yield
        finally:
          timestamp_end = time.perf_counter()
          duration = timestamp_end - timestamp_start
          with open(f"logs/{self.call_id}.json", "a") as f:
            f.write(json.dumps(self._get_structure(event, self.call_id, turn_id, duration, model)) + "\n")
