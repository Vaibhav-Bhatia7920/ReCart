import asyncio
import json
import sys
import time
from collections.abc import AsyncIterator, Callable
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from orchestrator.providers import DeepgramOrchestrator, agent_flow
from telemetry.logger import Logger
from voice.adapters.deepgram_asr import DeepgramASR
from voice.adapters.deepgram_tts import DeepgramTTS
from voice.errors import ASRProviderError, TTSProviderError
from voice.template import ASRBytes, FullTranscript, PartialTranscript

router = APIRouter()

_ASR_RATE = 16000
_TTS_RATE = 24000


def _websocket_can_send(websocket: WebSocket) -> bool:
    return websocket.client_state == WebSocketState.CONNECTED


async def _safe_send_json(websocket: WebSocket, payload: dict[str, Any]) -> bool:
    if not _websocket_can_send(websocket):
        return False
    try:
        await websocket.send_json(payload)
        return True
    except (WebSocketDisconnect, RuntimeError):
        return False


async def _safe_send_bytes(websocket: WebSocket, data: bytes) -> bool:
    if not _websocket_can_send(websocket):
        return False
    try:
        await websocket.send_bytes(data)
        return True
    except (WebSocketDisconnect, RuntimeError):
        return False


def default_asr_factory() -> DeepgramASR:
    return DeepgramASR()


def default_tts_factory() -> DeepgramTTS:
    return DeepgramTTS()


async def _pcm_stream(
    incoming: asyncio.Queue[bytes | None],
    call_id: str,
    turn_id: str,
) -> AsyncIterator[ASRBytes]:
    while True:
        chunk = await incoming.get()
        if chunk is None:
            return
        if chunk == b"":
            continue
        yield ASRBytes(bytes=chunk, timestamp=time.time(), call_id=call_id, turn_id=turn_id)


async def _forward_transcripts(
    transcript_stream: AsyncIterator[PartialTranscript | FullTranscript],
    websocket: WebSocket,
) -> AsyncIterator[PartialTranscript | FullTranscript]:
    """Mirror transcripts to the client while keeping the database_orchestrate chain intact."""
    async for transcript in transcript_stream:
        if isinstance(transcript, PartialTranscript):
            if not await _safe_send_json(websocket, {"type": "partial", "text": transcript.text}):
                return
        elif isinstance(transcript, FullTranscript):
            if not await _safe_send_json(websocket, {"type": "final", "text": transcript.text}):
                return
        yield transcript


@router.websocket("/ws/voice")
async def voice_socket(websocket: WebSocket, call_id: str = "browser-call", turn_id: str = "1") -> None:
    """Realtime voice over WebSocket using the database_orchestrate loop.

    PCM in (16 kHz linear16) → ASR → agent_flow → TTS → PCM out (24 kHz linear16).
    Same pipeline shape as ``database_orchestrate``, with live WS audio instead of files
    and WS playback instead of ``tts_local_consumer``.
    """
    await websocket.accept()
    asr_factory: Callable[[], Any] = getattr(websocket.app.state, "asr_factory", default_asr_factory)
    tts_factory: Callable[[], Any] = getattr(websocket.app.state, "tts_factory", default_tts_factory)
    run_agent_flow = getattr(websocket.app.state, "agent_flow", agent_flow)
    try:
        asr: Any = asr_factory()
        tts: Any = tts_factory()
    except (ASRProviderError, TTSProviderError) as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close(code=1011)
        return

    await websocket.send_json(
        {
            "type": "ready",
            "input_sample_rate": _ASR_RATE,
            "output_sample_rate": _TTS_RATE,
            "encoding": "linear16",
            "call_id": call_id,
            "turn_id": turn_id,
        }
    )
    incoming: asyncio.Queue[bytes | None] = asyncio.Queue()
    logger = Logger(call_id)
    orchestrator = DeepgramOrchestrator(asr, tts)
    session_ended = asyncio.Event()

    async def receive_client() -> None:
        try:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    await incoming.put(None)
                    session_ended.set()
                    return
                data = message.get("bytes")
                if data is not None:
                    await incoming.put(bytes(data))
                    continue
                text = message.get("text")
                if text is None:
                    continue
                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    await incoming.put(None)
                    session_ended.set()
                    return
                if payload.get("type") == "end":
                    await incoming.put(None)
                    session_ended.set()
                    return
        except WebSocketDisconnect:
            await incoming.put(None)
            session_ended.set()

    async def run_one_turn(active_turn_id: str) -> None:
        try:
            speech_to_text_stream = orchestrator.speech_to_text_live(
                _pcm_stream(incoming, call_id, active_turn_id),
                call_id,
                active_turn_id,
                logger,
            )
            client_visible = _forward_transcripts(speech_to_text_stream, websocket)
            agent_stream = run_agent_flow(client_visible, logger)
            text_to_speech_stream = orchestrator.agent_to_tts_orchestrate(
                agent_stream, call_id, active_turn_id, logger
            )

            sent_tts_start = False
            async for frame in text_to_speech_stream:
                if not frame:
                    continue
                if not sent_tts_start:
                    if not await _safe_send_json(
                        websocket,
                        {
                            "type": "tts_start",
                            "sample_rate": _TTS_RATE,
                            "encoding": "linear16",
                            "turn_id": active_turn_id,
                        },
                    ):
                        return
                    sent_tts_start = True
                if not await _safe_send_bytes(websocket, frame):
                    return
            if sent_tts_start:
                await _safe_send_json(
                    websocket, {"type": "tts_done", "turn_id": active_turn_id}
                )
        except WebSocketDisconnect:
            session_ended.set()
        except Exception as exc:
            print(f"voice_ws turn error: {exc!r}", file=sys.stdout, flush=True)
            await _safe_send_json(websocket, {"type": "error", "message": str(exc)})
            session_ended.set()

    def _turn_id_for_index(base_turn_id: str, index: int) -> str:
        if index <= 1:
            return base_turn_id
        if base_turn_id.isdigit():
            return str(int(base_turn_id) + index - 1)
        return f"{base_turn_id}-{index}"

    receiver = asyncio.create_task(receive_client())
    try:
        turn_index = 1
        while _websocket_can_send(websocket) and not session_ended.is_set():
            active_turn_id = _turn_id_for_index(turn_id, turn_index)
            await run_one_turn(active_turn_id)
            if session_ended.is_set():
                break
            turn_index += 1
            if not await _safe_send_json(
                websocket, {"type": "listening", "turn_id": _turn_id_for_index(turn_id, turn_index)}
            ):
                break
    finally:
        if not receiver.done():
            receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
        await _safe_send_json(websocket, {"type": "closed"})
