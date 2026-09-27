import asyncio
import audioop
import io
import time
import wave
from collections.abc import AsyncIterator
from pathlib import Path

from voice.interfaces import ASRProvider, TTSProvider
from voice.template import ASRBytes

_PCM_RATE = 16000
_CHUNK_BYTES = 3200  # 100 ms of 16 kHz 16-bit mono


def _wav_to_pcm16k_mono(wav_bytes: bytes) -> bytes:
    """Strip the WAV container and match DeepgramASR's linear16 / 16 kHz / mono socket."""
    with wave.open(io.BytesIO(wav_bytes), "rb") as wav:
        channels = wav.getnchannels()
        width = wav.getsampwidth()
        rate = wav.getframerate()
        frames = wav.readframes(wav.getnframes())
    if width != 2:
        frames = audioop.lin2lin(frames, width, 2)
        width = 2
    if channels == 2:
        frames = audioop.tomono(frames, width, 0.5, 0.5)
    elif channels != 1:
        raise ValueError(f"unsupported channel count: {channels}")
    if rate != _PCM_RATE:
        frames, _state = audioop.ratecv(frames, width, 1, rate, _PCM_RATE, None)
    return frames


class DeepgramOrchestrator:
    def __init__(self, asr_provider: ASRProvider, tts_provider: TTSProvider):
        self.asr_provider = asr_provider
        self.tts_provider = tts_provider

    async def get_audio_files(self) -> AsyncIterator[AsyncIterator[ASRBytes]]:
        audio_files_path = Path(__file__).resolve().parent.parent / "audio_db" / "audio_files"
        call_id = "123"
        for audio_file in sorted(audio_files_path.iterdir()):
            if not audio_file.is_file():
                continue
            pcm = _wav_to_pcm16k_mono(audio_file.read_bytes())
            yield _chunk_pcm(pcm, call_id=call_id, turn_id=audio_file.name)

    async def orchestrate(self) -> None:
        async for audio_chunks in self.get_audio_files():
            async for transcript in self.asr_provider.transcribe_audio(audio_chunks, "123", "456"):
                print(transcript)


async def _chunk_pcm(pcm: bytes, *, call_id: str, turn_id: str) -> AsyncIterator[ASRBytes]:
    stamp = time.time()
    for offset in range(0, len(pcm), _CHUNK_BYTES):
        yield ASRBytes(
            bytes=pcm[offset : offset + _CHUNK_BYTES],
            timestamp=stamp,
            call_id=call_id,
            turn_id=turn_id,
        )


if __name__ == "__main__":
    orchestrator = DeepgramOrchestrator(ASRProvider(), TTSProvider())
    asyncio.run(orchestrator.orchestrate())
