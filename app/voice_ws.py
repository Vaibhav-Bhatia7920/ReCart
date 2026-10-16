import asyncio
import json
import time
from collections.abc import AsyncIterator, Callable
from typing import Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from orchestrator.providers import DeepgramOrchestrator, agent_flow
from telemetry.logger import Logger
from voice.adapters.deepgram_asr import DeepgramASR
from voice.adapters.deepgram_tts import DeepgramTTS
from voice.errors import ASRProviderError, TTSProviderError
from voice.template import ASRBytes, FullTranscript, PartialTranscript

router = APIRouter()

_ASR_RATE = 16000
_TTS_RATE = 24000


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
            await websocket.send_json({"type": "partial", "text": transcript.text})
        elif isinstance(transcript, FullTranscript):
            await websocket.send_json({"type": "final", "text": transcript.text})
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

    async def receive_client() -> None:
        try:
            while True:
                message = await websocket.receive()
                if message["type"] == "websocket.disconnect":
                    await incoming.put(None)
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
                    return
                if payload.get("type") == "end":
                    await incoming.put(None)
                    return
        except WebSocketDisconnect:
            await incoming.put(None)

    async def run_database_style_turn() -> None:
        try:
            speech_to_text_stream = orchestrator.speech_to_text_live(
                _pcm_stream(incoming, call_id, turn_id),
                call_id,
                turn_id,
                logger,
            )
            client_visible = _forward_transcripts(speech_to_text_stream, websocket)
            agent_stream = run_agent_flow(client_visible, logger)
            text_to_speech_stream = orchestrator.agent_to_tts_orchestrate(
                agent_stream, call_id, turn_id, logger
            )

            sent_tts_start = False
            async for frame in text_to_speech_stream:
                if not frame:
                    continue
                if not sent_tts_start:
                    await websocket.send_json(
                        {"type": "tts_start", "sample_rate": _TTS_RATE, "encoding": "linear16"}
                    )
                    sent_tts_start = True
                await websocket.send_bytes(frame)
            if sent_tts_start:
                await websocket.send_json({"type": "tts_done"})
            await websocket.send_json({"type": "closed"})
        except Exception as exc:
            await websocket.send_json({"type": "error", "message": str(exc)})

    receiver = asyncio.create_task(receive_client())
    try:
        await run_database_style_turn()
    finally:
        if not receiver.done():
            receiver.cancel()
        await asyncio.gather(receiver, return_exceptions=True)
