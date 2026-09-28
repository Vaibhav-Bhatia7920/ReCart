import time
from typing import Any

from telemetry.helper_functions import mark_event_in_db

Event = str | dict[str, Any]


class TelemetryInstrument:
    """Call-scoped telemetry handle. Voice and graph code mark events through this, not the DB writer."""

    def __init__(self, name: str, description: str, call_id: str, turn_id: str) -> None:
        self.name = name
        self.description = description
        self.call_id = call_id
        self.turn_id = turn_id

    async def __aenter__(self) -> "TelemetryInstrument":
        self.start = time.time()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        self.end = time.time()
        self.duration = self.end - self.start

    async def mark_event(self, event: Event) -> None:
        await mark_event_in_db(self.call_id, event, self.turn_id)
