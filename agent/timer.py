from telemetry.instrument import TelemetryInstrument


class TurnTimer:
    """Times one graph node. Turn-log marks happen in commit_turn, not here."""

    def __init__(
        self,
        node: str,
        call_id: str,
        turn_id: str,
        instrument: TelemetryInstrument | None = None,
    ) -> None:
        self._node = node
        self._instrument = instrument or TelemetryInstrument(
            name=node,
            description=node,
            call_id=call_id,
            turn_id=turn_id,
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
