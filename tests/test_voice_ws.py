import asyncio
import json
from collections.abc import AsyncIterator

from fastapi import FastAPI
from fastapi.testclient import TestClient

from app.voice_ws import router as voice_ws_router


def _voice_test_app() -> FastAPI:
    """Voice WebSocket only — no app lifespan / Postgres / Redis."""
    test_app = FastAPI()
    test_app.include_router(voice_ws_router)
    return test_app
from voice.template import ASRBytes, FullTranscript, PartialTranscript


class StubASR:
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
        async for _chunk in audio_chunks:
            pass
        yield PartialTranscript(
            call_id=call_id,
            turn_id=turn_id,
            text="hello",
            timestamp=0.0,
            stability=0.5,
        )
        yield FullTranscript(
            call_id=call_id,
            turn_id=turn_id,
            text="hello there",
            end_time=1.0,
            confidence=0.9,
        )


class StubTTS:
    def __init__(self) -> None:
        self.spoken: list[str] = []

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
        del call_id, turn_id
        pieces: list[str] = []
        async for chunk in text:
            pieces.append(chunk)
        spoken = "".join(pieces)
        self.spoken.append(spoken)
        yield spoken.encode("utf-8")


class NoopVad:
    def feed(self, pcm: bytes) -> str | None:
        del pcm
        return None

    def reset(self) -> None:
        return


async def stub_agent_flow(transcript_stream, logger):
    """Stand-in for SimpleAgentStatelessV1: reply with a fixed agent line."""
    del logger
    async for transcript in transcript_stream:
        if isinstance(transcript, FullTranscript):
            transcript.text = "I can help with that."
            yield transcript


def test_voice_websocket_runs_database_orchestrate_style_turn() -> None:
    tts = StubTTS()
    with TestClient(_voice_test_app()) as starlette_client:
        starlette_client.app.state.asr_factory = lambda: StubASR()
        starlette_client.app.state.tts_factory = lambda: tts
        starlette_client.app.state.agent_flow = stub_agent_flow
        starlette_client.app.state.vad_monitor_factory = NoopVad
        with starlette_client.websocket_connect("/ws/voice?call_id=call-ws&turn_id=1") as socket:
            ready = socket.receive_json()
            assert ready["type"] == "ready"
            assert ready["input_sample_rate"] == 16000
            assert ready["output_sample_rate"] == 24000
            assert ready["aec"]["mode"] == "browser"
            assert ready["aec"]["playback_same_audio_context"] is True
            socket.send_text(
                '{"type":"client_hello","aec":{"echoCancellation":true}}'
            )
            socket.send_bytes(bytes(3200))
            socket.send_text('{"type":"end"}')
            messages: list[object] = []
            while True:
                raw = socket.receive()
                if "bytes" in raw:
                    messages.append(raw["bytes"])
                    continue
                payload = json.loads(raw["text"])
                messages.append(payload)
                if payload["type"] in {"closed", "error"}:
                    break
    kinds = [item["type"] if isinstance(item, dict) else "audio" for item in messages]
    assert kinds == ["partial", "final", "tts_start", "audio", "tts_done", "closed"]
    assert messages[0] == {"type": "partial", "text": "hello"}
    assert messages[1] == {"type": "final", "text": "hello there"}
    assert messages[3] == b"I can help with that."
    assert tts.spoken == ["I can help with that."]


class BargeInVad:
    """Triggers speech-start on the second uplink chunk while TTS is active."""

    def __init__(self) -> None:
        self._chunks = 0

    def feed(self, pcm: bytes) -> str | None:
        del pcm
        self._chunks += 1
        if self._chunks == 2:
            return "start"
        return None

    def reset(self) -> None:
        self._chunks = 0


class BargeInStubASR(StubASR):
    """Finalize after two uplink chunks so the test can send more audio during TTS."""

    async def _transcribe(self, audio_chunks, call_id, turn_id):
        seen = 0
        async for _chunk in audio_chunks:
            seen += 1
            if seen == 1:
                yield PartialTranscript(
                    call_id=call_id,
                    turn_id=turn_id,
                    text="hello",
                    timestamp=0.0,
                    stability=0.5,
                )
            if seen >= 2:
                yield FullTranscript(
                    call_id=call_id,
                    turn_id=turn_id,
                    text="hello there",
                    end_time=1.0,
                    confidence=0.9,
                )
                return


class SlowStubTTS(StubTTS):
    def __init__(self) -> None:
        super().__init__()
        self.release_second_frame = asyncio.Event()

    async def _synthesize(self, text, call_id, turn_id):
        del call_id, turn_id
        pieces: list[str] = []
        async for chunk in text:
            pieces.append(chunk)
        spoken = "".join(pieces)
        self.spoken.append(spoken)
        yield b"part-a"
        await self.release_second_frame.wait()
        yield b"part-b"


def test_voice_websocket_barge_in_cancels_tts_turn() -> None:
    tts = SlowStubTTS()
    with TestClient(_voice_test_app()) as starlette_client:
        starlette_client.app.state.asr_factory = lambda: BargeInStubASR()
        starlette_client.app.state.tts_factory = lambda: tts
        starlette_client.app.state.agent_flow = stub_agent_flow
        starlette_client.app.state.vad_monitor_factory = BargeInVad
        with starlette_client.websocket_connect("/ws/voice?call_id=call-ws&turn_id=1") as socket:
            ready = socket.receive_json()
            assert ready["type"] == "ready"
            socket.send_bytes(bytes(3200))
            socket.send_bytes(bytes(3200))
            while True:
                raw = socket.receive()
                if "bytes" in raw:
                    continue
                payload = json.loads(raw["text"])
                if payload["type"] == "tts_start":
                    break
            socket.send_bytes(bytes(3200))
            socket.send_bytes(bytes(3200))
            tts.release_second_frame.set()
            socket.send_text('{"type":"end"}')
            messages: list[object] = []
            while True:
                raw = socket.receive()
                if "bytes" in raw:
                    messages.append(raw["bytes"])
                    continue
                payload = json.loads(raw["text"])
                messages.append(payload)
                if payload["type"] in {"closed", "error"}:
                    break
    kinds = [item["type"] if isinstance(item, dict) else "audio" for item in messages]
    assert "tts_interrupt" in kinds
    assert kinds[-1] == "closed"
