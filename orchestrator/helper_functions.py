import wave
import io
import audioop
import subprocess
from typing import AsyncIterator
from voice.template import ASRBytes
from pathlib import Path
import time

_PCM_RATE = 16000
_CHUNK_BYTES = 1024  # 100 ms of 16 kHz 16-bit mono
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



def _mp3_to_pcm16k_mono(mp3_bytes: bytes) -> bytes:
    """Decode MP3 bytes and convert to linear16 / 16 kHz / mono PCM for Deepgram."""
    cmd = [
        "ffmpeg",
        "-nostdin",
        "-threads",
        "0",
        "-i",
        "pipe:0",  # Read input from stdin
        "-f",
        "s16le",  # Output raw signed 16-bit little-endian PCM
        "-acodec",
        "pcm_s16le",
        "-ar",
        str(_PCM_RATE),  # Resample to 16 kHz
        "-ac",
        "1",  # Downmix to mono
        "pipe:1",  # Write output to stdout
    ]

    process = subprocess.Popen(
        cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )

    pcm_bytes, stderr = process.communicate(input=mp3_bytes)

    if process.returncode != 0:
        raise RuntimeError(f"FFmpeg decoding failed: {stderr.decode(errors='replace')}")

    return pcm_bytes

async def _chunk_pcm(pcm: bytes, *, call_id: str, turn_id: str) -> AsyncIterator[ASRBytes]:
    stamp = time.time()
    for offset in range(0, len(pcm), _CHUNK_BYTES):
        yield pcm[offset : offset + _CHUNK_BYTES]

async def get_audio_files(call_id: str, turn_id: str) -> AsyncIterator[AsyncIterator[ASRBytes]]:
        audio_files_path = Path(__file__).parent.parent / "audio_db" / "audio_files" / "human-nonhuman" / "human" 
  
        for audio_file in sorted((audio_files_path.iterdir()))[:1]:
            
            if not audio_file.is_file() or audio_file.suffix.lower() not in [".wav", ".mp3"]:
                continue
            if audio_file.suffix.lower() == ".mp3":
                print("speech to text orchestrate got mp3 file")
                pcm = _mp3_to_pcm16k_mono(audio_file.read_bytes())
            else:   
                pcm = _wav_to_pcm16k_mono(audio_file.read_bytes())
            yield _chunk_pcm(pcm, call_id=call_id, turn_id=turn_id)