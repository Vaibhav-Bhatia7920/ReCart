import asyncio
import time
from typing import AsyncIterator

from telemetry.helper_functions import context_manager, mark_event_in_db
from voice.template import ASR_Bytes


class TelemetryInstrument:
    def __init__(self, name: str, description: str, input_audio: AsyncIterator[ASR_Bytes]):
        self.name = name
        self.description = description
        self.input_audio = input_audio

    async def __aenter__(self) -> "TelemetryInstrument":
        self.start = time.time()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback) -> None:
        self.end = time.time()
        self.duration = self.end - self.start

    async def mark_event(self, event: str) -> None:
        async for audio in context_manager(self.input_audio):
            await mark_event_in_db(audio, event)
            await asyncio.sleep(1)
