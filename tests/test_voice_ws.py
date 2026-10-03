import json
from collections.abc import AsyncIterator

from fastapi.testclient import TestClient

from app.main import app
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


def test_voice_websocket_transcribes_then_speaks() -> None:
    tts = StubTTS()
    with TestClient(app) as starlette_client:
        starlette_client.app.state.asr_factory = lambda: StubASR()
        starlette_client.app.state.tts_factory = lambda: tts
        with starlette_client.websocket_connect("/ws/voice?call_id=call-ws&turn_id=1") as socket:
            ready = socket.receive_json()
            assert ready["type"] == "ready"
            assert ready["input_sample_rate"] == 16000
            assert ready["output_sample_rate"] == 24000
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
    assert messages[1] == {"type": "final", "text": "hello there"}
    print(messages[3])
    print(b"hello there")
    assert messages[3] == b"hello there"
    assert tts.spoken == ["hello there"]
