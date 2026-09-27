import time
from collections.abc import AsyncIterator

from telemetry.instrument import TelemetryInstrument
from voice.template import ASR_Bytes


async def _turn_audio(call_id: str, turn_id: str) -> AsyncIterator[ASR_Bytes]:
    yield ASR_Bytes(bytes=b"", timestamp=time.time(), call_id=call_id, turn_id=turn_id)


class TurnTimer:
    """Times one graph node with TelemetryInstrument. The agent does not write events itself."""

    def __init__(
        self,
        node: str,
        call_id: str,
        turn_id: str,
        instrument: TelemetryInstrument | None = None,
    ) -> None:
        self._instrument = instrument or TelemetryInstrument(
            name=node,
            description=node,
            input_audio=_turn_audio(call_id, turn_id),
        )

    async def __aenter__(self) -> TelemetryInstrument:
        return await self._instrument.__aenter__()

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: object,
    ) -> None:
        await self._instrument.__aexit__(exc_type, exc_value, traceback)
        await self._instrument.mark_event(self._instrument.name)
