import asyncio
import json
import sys
import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState

from orchestrator.providers import DeepgramOrchestrator, agent_flow
from orchestrator.vad_detector import SileroVadMonitor
from telemetry.logger import Logger
from voice.adapters.deepgram_asr import DeepgramASR
from voice.adapters.deepgram_tts import DeepgramTTS
from voice.errors import ASRProviderError, TTSProviderError
from voice.template import ASRBytes, FullTranscript, PartialTranscript

router = APIRouter()

_ASR_RATE = 16000
_TTS_RATE = 24000
# Browser tabs should capture with getUserMedia echoCancellation and play TTS through the
# same AudioContext so Chrome/Safari can subtract speaker bleed from the uplink.
_AEC_MODE = "browser"


class VadMonitor(Protocol):
    def feed(self, pcm: bytes) -> str | None: ...

    def reset(self) -> None: ...


def default_vad_monitor_factory() -> VadMonitor:
    return SileroVadMonitor()


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


def _turn_id_for_index(base_turn_id: str, index: int) -> str:
    if index <= 1:
        return base_turn_id
    if base_turn_id.isdigit():
        return str(int(base_turn_id) + index - 1)
    return f"{base_turn_id}-{index}"


async def _send_ready(
    websocket: WebSocket,
    *,
    call_id: str,
    turn_id: str,
) -> None:
    await websocket.send_json(
        {
            "type": "ready",
            "input_sample_rate": _ASR_RATE,
            "output_sample_rate": _TTS_RATE,
            "encoding": "linear16",
            "call_id": call_id,
            "turn_id": turn_id,
            "aec": {
                "mode": _AEC_MODE,
                "capture_constraints": {
                    "echoCancellation": True,
                    "noiseSuppression": True,
                    "autoGainControl": True,
                },
                "playback_same_audio_context": True,
            },
        }
    )


async def _stream_tts_frames(
    websocket: WebSocket,
    text_to_speech_stream: AsyncIterator[bytes],
    active_turn_id: str,
    tts_active: asyncio.Event,
) -> None:
    sent_tts_start = False
    try:
        async for frame in text_to_speech_stream:
            if not frame:
                continue
            if not tts_active.is_set() and sent_tts_start:
                return
            if not sent_tts_start:
                if not await _safe_send_json(
                    websocket,
                    {
                        "type": "tts_start",
                        "sample_rate": _TTS_RATE,
                        "encoding": "linear16",
                        "turn_id": active_turn_id,
                        "playback": {"role": "aec_reference"},
                    },
                ):
                    return
                sent_tts_start = True
                tts_active.set()
            if not await _safe_send_bytes(websocket, frame):
                return
    finally:
        tts_active.clear()
    if sent_tts_start:
        await _safe_send_json(websocket, {"type": "tts_done", "turn_id": active_turn_id})


@dataclass
class VoiceSession:
    websocket: WebSocket
    call_id: str
    base_turn_id: str
    incoming: asyncio.Queue[bytes | None]
    logger: Logger
    orchestrator: DeepgramOrchestrator
    run_agent_flow: Callable[..., Any]
    session_ended: asyncio.Event = field(default_factory=asyncio.Event)
    tts_active: asyncio.Event = field(default_factory=asyncio.Event)
    current_turn_task: asyncio.Task[None] | None = None
    active_turn_id: str = ""
    vad: VadMonitor = field(default_factory=default_vad_monitor_factory)


async def _handle_barge_in(session: VoiceSession, active_turn_id: str) -> None:
    session.tts_active.clear()
    session.orchestrator.interrupt()
    session.vad.reset()
    await _safe_send_json(
        session.websocket,
        {"type": "tts_interrupt", "turn_id": active_turn_id},
    )
    turn_task = session.current_turn_task
    if turn_task is not None and not turn_task.done():
        turn_task.cancel()


async def _receive_client(session: VoiceSession) -> None:
    websocket = session.websocket
    incoming = session.incoming
    logger = session.logger
    call_id = session.call_id
    base_turn_id = session.base_turn_id
    try:
        while True:
            message = await websocket.receive()
            if message["type"] == "websocket.disconnect":
                await incoming.put(None)
                session.session_ended.set()
                return
            data = message.get("bytes")
            if data is not None:
                chunk = bytes(data)
                if session.tts_active.is_set():
                    vad_event = session.vad.feed(chunk)
                    if vad_event == "start" and session.active_turn_id:
                        await _handle_barge_in(session, session.active_turn_id)
                await incoming.put(chunk)
                continue
            text = message.get("text")
            if text is None:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                await incoming.put(None)
                session.session_ended.set()
                return
            msg_type = payload.get("type")
            if msg_type == "client_hello":
                if payload.get("aec"):
                    logger.log_event("Client AEC Hello", call_id, base_turn_id)
                continue
            if msg_type == "end":
                await incoming.put(None)
                session.session_ended.set()
                return
    except WebSocketDisconnect:
        await incoming.put(None)
        session.session_ended.set()


async def _run_voice_turn(session: VoiceSession, active_turn_id: str) -> None:
    websocket = session.websocket
    orchestrator = session.orchestrator
    run_agent_flow = session.run_agent_flow
    call_id = session.call_id
    logger = session.logger
    try:
        speech_to_text_stream = orchestrator.speech_to_text_live(
            _pcm_stream(session.incoming, call_id, active_turn_id),
            call_id,
            active_turn_id,
            logger,
        )
        client_visible = _forward_transcripts(speech_to_text_stream, websocket)
        agent_stream = run_agent_flow(client_visible, logger)
        text_to_speech_stream = orchestrator.agent_to_tts_orchestrate(
            agent_stream, call_id, active_turn_id, logger
        )
        await _stream_tts_frames(
            websocket, text_to_speech_stream, active_turn_id, session.tts_active
        )
    except asyncio.CancelledError:
        session.tts_active.clear()
        raise
    except WebSocketDisconnect:
        session.session_ended.set()
    except Exception as exc:
        print(f"voice_ws turn error: {exc!r}", file=sys.stdout, flush=True)
        await _safe_send_json(websocket, {"type": "error", "message": str(exc)})
        session.session_ended.set()


async def _run_voice_session(session: VoiceSession) -> None:
    websocket = session.websocket
    base_turn_id = session.base_turn_id
    receiver = asyncio.create_task(_receive_client(session))
    try:
        turn_index = 1
        while _websocket_can_send(websocket) and not session.session_ended.is_set():
            session.orchestrator.reset_interrupt()
            session.vad.reset()
            active_turn_id = _turn_id_for_index(base_turn_id, turn_index)
            session.active_turn_id = active_turn_id
            turn_task = asyncio.create_task(_run_voice_turn(session, active_turn_id))
            session.current_turn_task = turn_task
            try:
                await turn_task
            except asyncio.CancelledError:
                turn_index += 1
                continue
            finally:
                session.current_turn_task = None
                session.active_turn_id = ""
            if session.session_ended.is_set():
                break
            turn_index += 1
            if not await _safe_send_json(
                websocket,
                {"type": "listening", "turn_id": _turn_id_for_index(base_turn_id, turn_index)},
            ):
                break
    finally:
        if not receiver.done():
            receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
        await _safe_send_json(websocket, {"type": "closed"})


@router.websocket("/ws/voice")
async def voice_socket(websocket: WebSocket, call_id: str = "browser-call", turn_id: str = "1") -> None:
    """Realtime voice over WebSocket using the database_orchestrate loop.

    PCM in (16 kHz linear16) → ASR → agent_flow → TTS → PCM out (24 kHz linear16).
    Silero VAD on uplink during TTS triggers barge-in via ``DeepgramOrchestrator.interrupt()``.

    Client-side AEC: web clients should enable ``echoCancellation`` (and related constraints)
    on ``getUserMedia``, play assistant PCM through the same ``AudioContext`` as the mic
    graph (so the browser has a playback reference), and avoid routing the mic to speakers.
    Optional JSON ``{"type":"client_hello","aec":{...}}`` after ``ready`` is accepted and ignored
    by the pipeline (for logging / future tuning).
    """
    await websocket.accept()
    asr_factory: Callable[[], Any] = getattr(websocket.app.state, "asr_factory", default_asr_factory)
    tts_factory: Callable[[], Any] = getattr(websocket.app.state, "tts_factory", default_tts_factory)
    run_agent_flow = getattr(websocket.app.state, "agent_flow", agent_flow)
    vad_factory: Callable[[], VadMonitor] = getattr(
        websocket.app.state, "vad_monitor_factory", default_vad_monitor_factory
    )
    try:
        asr: Any = asr_factory()
        tts: Any = tts_factory()
    except (ASRProviderError, TTSProviderError) as exc:
        await websocket.send_json({"type": "error", "message": str(exc)})
        await websocket.close(code=1011)
        return

    await _send_ready(websocket, call_id=call_id, turn_id=turn_id)
    session = VoiceSession(
        websocket=websocket,
        call_id=call_id,
        base_turn_id=turn_id,
        incoming=asyncio.Queue(),
        logger=Logger(call_id),
        orchestrator=DeepgramOrchestrator(asr, tts),
        run_agent_flow=run_agent_flow,
        vad=vad_factory(),
    )
    await _run_voice_session(session)
