"""Deepgram Aura-2 streaming TTS. Implements voice.interfaces.TTSProvider."""

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Final
from urllib.parse import urlencode

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from telemetry.logger import Logger
from telemetry.instrument import TelemetryInstrument
from voice.errors import TTSConnectionDropped, TTSProviderError, TTSTimeoutError

PROVIDER: Final = "deepgram"
_SPEAK_URL: Final = "wss://api.deepgram.com/v1/speak"
_MODEL: Final = "aura-2-asteria-en"
_ENCODING: Final = "linear16"
_SAMPLE_RATE: Final = 24000
_CONNECT_TIMEOUT_SECONDS: Final = 10.0
_RECONNECT_BACKOFF_SECONDS: Final = 0.5
_CLOSE: Final = '{"type":"Close"}'


class _SendState:
    """Text taken from the queue but not yet accepted by the socket."""

    def __init__(self) -> None:
        self.pending: str | None = None
        self.has_pending = False
        self.close_sent = False


class _StreamDropped(Exception):
    """Socket closed before Close was sent. Triggers the single reconnect."""


class DeepgramTTS:
    """Streams text to Deepgram speak and yields raw PCM audio bytes.

    Output is linear16 at 24 kHz, as declared on the socket. This class does not resample.
    """

    def __init__(self, api_key: str | None = None) -> None:
        key = api_key if api_key is not None else os.environ.get("DEEPGRAM_API_KEY")
        if not key:
            raise TTSProviderError("DEEPGRAM_API_KEY is not set", provider=PROVIDER)
        self._api_key = key

    def synthesize_audio(
        self,
        text: AsyncIterator[str],
        call_id: str,
        turn_id: str,
    ) -> AsyncIterator[bytes]:
        return self._synthesize(text, call_id, turn_id)

    async def _synthesize(
        self,
        text: AsyncIterator[str],
        call_id: str,
        turn_id: str,
    ) -> AsyncIterator[bytes]:
        outgoing: asyncio.Queue[str | None] = asyncio.Queue()
        state = _SendState()
        seen_audio = False
        telemetry = TelemetryInstrument(
            name="deepgram_tts",
            description="Deepgram Aura-2 streaming TTS",
            call_id=call_id,
            turn_id=turn_id,
        )

        async def read_text() -> None:
            try:
                async for chunk in text:
                    await outgoing.put(chunk)
            finally:
                await outgoing.put(None)

        reader = asyncio.create_task(read_text())
        reconnects_left = 1
        try:
            while True:
                _raise_if_reader_failed(reader)
                try:
                    async for audio in self._session(outgoing, state, seen_audio, telemetry):
                        seen_audio = True
                        yield audio
                    return
                except _StreamDropped as dropped:
                    if reconnects_left <= 0:
                        raise TTSConnectionDropped(
                            "Deepgram WebSocket dropped after one reconnect",
                            provider=PROVIDER,
                        ) from dropped.__cause__
                    reconnects_left -= 1
                    await asyncio.sleep(_RECONNECT_BACKOFF_SECONDS)
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _session(
        self,
        outgoing: asyncio.Queue[str | None],
        state: _SendState,
        seen_audio: bool,
        telemetry: TelemetryInstrument,
    ) -> AsyncIterator[bytes]:
        logger = Logger()
        logger.info("TTS Session Started", extra={"extra_fields": {"call_id": call_id, "turn_id": turn_id, "model": "deepgram-aura-2", "start_time": time.perf_counter()}})
        query = urlencode(
            {
                "model": _MODEL,
                "encoding": _ENCODING,
                "sample_rate": str(_SAMPLE_RATE),
                "container": "none",
            }
        )
        try:
            async with connect(
                f"{_SPEAK_URL}?{query}",
                additional_headers={"Authorization": f"Token {self._api_key}"},
                open_timeout=_CONNECT_TIMEOUT_SECONDS,
            ) as websocket:
                pump = asyncio.create_task(_send_text(websocket, outgoing, state))
                try:
                    async for raw in websocket:
                        if isinstance(raw, bytes):
                            if raw == b"":
                                continue
                            if not seen_audio:
                                seen_audio = True
                                # log_event(call_id, "First Audio Frame", turn_id)
                                await telemetry.mark_event("First Audio Frame")
                                # TELEMETRY HOOK: mark "first_audio" — first TTS audio frame received
                            yield raw
                            continue
                        kind = _control_type(raw)
                        if kind == "Flushed":
                            # log_event(call_id, "Flushed", turn_id)
                            await telemetry.mark_event("Flushed")
                            # TELEMETRY HOOK: mark "flushed" — Deepgram finished audio for text sent so far
                            continue
                    pump_error = pump.exception() if pump.done() else None
                    if not state.close_sent or isinstance(pump_error, ConnectionClosed):
                        raise _StreamDropped() from (pump_error if isinstance(pump_error, Exception) else None)
                    if pump_error is not None:
                        raise pump_error
                except ConnectionClosed as exc:
                    raise _StreamDropped() from exc
                finally:
                    if not pump.done():
                        pump.cancel()
                    await asyncio.gather(pump, return_exceptions=True)
        except TimeoutError as exc:
            raise TTSTimeoutError(
                f"Deepgram connection timed out after {_CONNECT_TIMEOUT_SECONDS} seconds",
                provider=PROVIDER,
            ) from exc
        except InvalidStatus as exc:
            raise TTSProviderError(
                f"Deepgram rejected the WebSocket handshake ({exc})",
                provider=PROVIDER,
            ) from exc
        finally:
            logger.info("TTS Session Ended", extra={"extra_fields": {"call_id": call_id, "turn_id": turn_id, "model": "deepgram-aura-2", "end_time": time.perf_counter()}})


def _raise_if_reader_failed(reader: asyncio.Task[None]) -> None:
    if not reader.done():
        return
    error = reader.exception()
    if error is not None:
        raise error


async def _send_text(
    websocket: ClientConnection,
    outgoing: asyncio.Queue[str | None],
    state: _SendState,
) -> None:
    while True:
        if not state.has_pending:
            state.pending = await outgoing.get()
            state.has_pending = True
        payload = state.pending
        if payload is None:
            await websocket.send(_CLOSE)
            state.close_sent = True
            state.has_pending = False
            state.pending = None
            return
        if payload.strip() == "":
            state.has_pending = False
            state.pending = None
            continue
        await websocket.send(json.dumps({"type": "Speak", "text": payload}))
        state.has_pending = False
        state.pending = None


def _control_type(raw: str) -> str:
    message = _object_dict(raw)
    kind = message.get("type")
    if kind == "Warning":
        return "Warning"
    if not isinstance(kind, str):
        return ""
    return kind


def _object_dict(raw: str) -> dict[str, object]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise TTSProviderError("Deepgram sent a non-JSON text frame", provider=PROVIDER) from exc
    if not isinstance(parsed, dict):
        raise TTSProviderError("Deepgram sent a non-object text frame", provider=PROVIDER)
    result: dict[str, object] = {}
    for key, value in parsed.items():
        result[str(key)] = value
    return result
