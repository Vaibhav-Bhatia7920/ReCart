"""Deepgram Nova-3 streaming ASR. Implements voice.interfaces.ASRProvider."""

import asyncio
import json
import os
from collections.abc import AsyncIterator
from typing import Final
from urllib.parse import urlencode

from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import ConnectionClosed, InvalidStatus

from voice.errors import ASRConnectionDropped, ASRProviderError, ASRTimeoutError
from voice.template import ASRBytes, FullTranscript, PartialTranscript
from telemetry.instrument import TelemetryInstrument
from telemetry.helper_functions import mark_event_in_db

PROVIDER: Final = "deepgram"
_LISTEN_URL: Final = "wss://api.deepgram.com/v1/listen"
_MODEL: Final = "nova-3"
_ENCODING: Final = "linear16"
_SAMPLE_RATE: Final = 16000
_CHANNELS: Final = 1
_CONNECT_TIMEOUT_SECONDS: Final = 10.0
_RECONNECT_BACKOFF_SECONDS: Final = 0.5
_CLOSE_STREAM: Final = '{"type":"CloseStream"}'


class _SendState:
    """Bytes taken from the audio queue but not yet accepted by the socket."""

    def __init__(self) -> None:
        self.pending: bytes | None = None
        self.has_pending = False
        self.audio_done = False


class _StreamDropped(Exception):
    """Socket closed before CloseStream was sent. Triggers the single reconnect."""


class DeepgramASR:
    """Streams raw PCM to Deepgram and yields interim and segment-final transcripts.

    Audio bytes are sent as-is. The socket declares linear16, 16 kHz, mono.
    This class does not resample.
    """

    def __init__(self, api_key: str | None = None) -> None:
        key = api_key if api_key is not None else os.environ.get("DEEPGRAM_API_KEY")
        if not key:
            raise ASRProviderError("DEEPGRAM_API_KEY is not set", provider=PROVIDER)
        self._api_key = key

    def transcribe_audio(
        self,
        audio_chunks: AsyncIterator[ASRBytes],
        call_id: str,
        turn_id: str,
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        return self._transcribe(audio_chunks, call_id, turn_id)

    async def _transcribe(
        self,
        audio_chunks: AsyncIterator[ASRBytes],
        call_id: str,
        turn_id: str,
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        outgoing: asyncio.Queue[bytes | None] = asyncio.Queue()
        state = _SendState()
        seen_partial = False

        if not hasattr(audio_chunks, "__aiter__"):
            raise TypeError(
                "transcribe_audio expects AsyncIterator[ASRBytes], not "
                f"{type(audio_chunks).__name__}. Passing a WAV file as bytes "
                "sends CloseStream with no audio; Deepgram then returns only "
                "Metadata with an empty sha256."
            )

        async def read_audio() -> None:
            try:
                async for chunk in audio_chunks:
                    payload = chunk.bytes if isinstance(chunk, ASRBytes) else chunk
                    if not isinstance(payload, bytes | bytearray):
                        raise TypeError(
                            "audio chunks must be ASRBytes (or bytes), "
                            f"got {type(chunk).__name__}"
                        )
                    await outgoing.put(bytes(payload))
            finally:
                await outgoing.put(None)

        reader = asyncio.create_task(read_audio())
        reconnects_left = 1
        print("--------------------------------")
        print("Starting transcription")
        print("--------------------------------")
        try:
            while True:
                print("--------------------------------")
                print("Reader: ", reader)
                print("--------------------------------")
                _raise_if_reader_failed(reader)
                try:
                    print("--------------------------------")
                    print("Starting session")
                    print("--------------------------------")
                    async for transcript in self._session(
                        outgoing, state, call_id, turn_id, seen_partial, reader
                    ):
                        print("--------------------------------")
                        print("Transcript: ", transcript)
                        print("--------------------------------")
                        if isinstance(transcript, PartialTranscript):
                            print("--------------------------------")
                            print("Seen partial: ", seen_partial)
                            print("--------------------------------")
                            seen_partial = True
                        yield transcript
                    return
                except _StreamDropped as dropped:
                    if reconnects_left <= 0:
                        raise ASRConnectionDropped(
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
        outgoing: asyncio.Queue[bytes | None],
        state: _SendState,
        call_id: str,
        turn_id: str,
        seen_partial: bool,
        reader: asyncio.Task[None],
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        query = urlencode(
            {
                "model": _MODEL,
                "language": "en",
                "encoding": _ENCODING,
                "sample_rate": str(_SAMPLE_RATE),
                "channels": str(_CHANNELS),
                "interim_results": "true",
                "punctuate": "true",
            }
        )
        try:
            async with connect(
                f"{_LISTEN_URL}?{query}",
                additional_headers={"Authorization": f"Token {self._api_key}"},
                open_timeout=_CONNECT_TIMEOUT_SECONDS,
            ) as websocket:
                pump = asyncio.create_task(_send_audio(websocket, outgoing, state))
                try:
                    print("--------------------------------")
                    print("Starting to receive transcripts")
                    print("--------------------------------")
                    async for raw in websocket:
                        _raise_if_reader_failed(reader)
                        print("--------------------------------")
                        print("Raw: ", raw)
                        print("--------------------------------")
                        for transcript, is_final in _transcripts_from_message(raw, call_id, turn_id):
                            print("--------------------------------")
                            print("Transcript: ", transcript)
                            print("--------------------------------")
                            print("Is final: ", is_final)
                            print("--------------------------------")
                            if is_final:
                                await mark_event_in_db(transcript.audio, "Final Transcript")
                                # TELEMETRY HOOK: mark "final" — Deepgram signals is_final=true
                                yield transcript
                            else:
                                if not seen_partial:
                                    seen_partial = True
                                    await mark_event_in_db(transcript.audio, "First Partial Transcript")
                                    # TELEMETRY HOOK: mark "first_partial" — first interim result received
                                yield transcript
                    pump_error = pump.exception() if pump.done() else None
                    if not state.audio_done or isinstance(pump_error, ConnectionClosed):
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
            raise ASRTimeoutError(
                f"Deepgram connection timed out after {_CONNECT_TIMEOUT_SECONDS} seconds",
                provider=PROVIDER,
            ) from exc
        except InvalidStatus as exc:
            raise ASRProviderError(
                f"Deepgram rejected the WebSocket handshake ({exc})",
                provider=PROVIDER,
            ) from exc


def _raise_if_reader_failed(reader: asyncio.Task[None]) -> None:
    if not reader.done():
        return
    error = reader.exception()
    if error is not None:
        raise error


async def _send_audio(
    websocket: ClientConnection,
    outgoing: asyncio.Queue[bytes | None],
    state: _SendState,
) -> None:
    while True:
        if not state.has_pending:
            state.pending = await outgoing.get()
            state.has_pending = True
        payload = state.pending
        if payload is None:
            await websocket.send(_CLOSE_STREAM)
            state.audio_done = True
            state.has_pending = False
            state.pending = None
            return
        await websocket.send(payload)
        state.has_pending = False
        state.pending = None


def _transcripts_from_message(
    raw: str | bytes,
    call_id: str,
    turn_id: str,
) -> list[tuple[PartialTranscript | FullTranscript, bool]]:
    message = _object_dict(raw)
    print("--------------------------------")
    print("Message: ", message)
    print("--------------------------------")
    message_type = message.get("type")
    print("--------------------------------")
    print("Message type: ", message_type)
    print("--------------------------------")
    if message_type == "Error":
        # The published Results schema does not document this type. If Deepgram
        # sends it, fail the stream instead of ignoring an error frame.
        raise ASRProviderError(f"Deepgram error frame: {_text(raw)}", provider=PROVIDER)
    if message_type != "Results":
        return []

    # speech_final is the utterance-pause flag. Segment finality is is_final.
    is_final = message.get("is_final") is True
    text = _alternative_transcript(message.get("channel"))
    if text is None:
        return []
    confidence = _alternative_confidence(message.get("channel"))
    start = _number(message.get("start"))
    if is_final:
        end_time = start + _number(message.get("duration"))
        transcript: PartialTranscript | FullTranscript = FullTranscript(
            call_id=call_id,
            turn_id=turn_id,
            text=text,
            end_time=end_time,
            confidence=confidence,
        )
    else:
        transcript = PartialTranscript(
            call_id=call_id,
            turn_id=turn_id,
            text=text,
            timestamp=start,
            stability=min(1.0, max(0.0, confidence)),
        )
    return [(transcript, is_final)]


def _object_dict(raw: str | bytes) -> dict[str, object]:
    text = _text(raw)
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise ASRProviderError("Deepgram sent a non-JSON message", provider=PROVIDER) from exc
    if not isinstance(parsed, dict):
        raise ASRProviderError("Deepgram sent a non-object message", provider=PROVIDER)
    result: dict[str, object] = {}
    for key, value in parsed.items():
        result[str(key)] = value
    return result


def _text(raw: str | bytes) -> str:
    if isinstance(raw, str):
        return raw
    try:
        return raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ASRProviderError("Deepgram sent a non-UTF8 message", provider=PROVIDER) from exc


def _alternative_transcript(channel: object) -> str | None:
    alternative = _first_alternative(channel)
    if alternative is None:
        return None
    text = alternative.get("transcript")
    if not isinstance(text, str) or text == "":
        return None
    return text


def _alternative_confidence(channel: object) -> float:
    alternative = _first_alternative(channel)
    if alternative is None:
        return 0.0
    return _number(alternative.get("confidence"))


def _first_alternative(channel: object) -> dict[str, object] | None:
    if not isinstance(channel, dict):
        return None
    alternatives = channel.get("alternatives")
    if not isinstance(alternatives, list) or not alternatives:
        return None
    first = alternatives[0]
    if not isinstance(first, dict):
        return None
    parsed: dict[str, object] = {}
    for key, value in first.items():
        parsed[str(key)] = value
    return parsed


def _number(value: object) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float):
        return 0.0
    return float(value)
