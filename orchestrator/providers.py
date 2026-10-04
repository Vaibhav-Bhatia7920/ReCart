import asyncio
import audioop
import io
import time
import wave
from collections.abc import AsyncIterator
from pathlib import Path

import os
import numpy as np
import sounddevice as sd
import queue as std_queue
from voice.interfaces import ASRProvider, TTSProvider
from voice.template import ASRBytes, FullTranscript, PartialTranscript
from app.db import init_engine

_PCM_RATE = 16000
_CHUNK_BYTES = 3200  # 100 ms of 16 kHz 16-bit mono
_SAMPLE_RATE = 16000
_BLOCK_SIZE = 1024
_CHANNELS = 1
from dotenv import load_dotenv

load_dotenv()

async def mic_streamer(queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):

    def callback(indata, frames, time, status):
        audio_chunk = indata.copy()
        loop.call_soon_threadsafe(queue.put_nowait, audio_chunk)

    stream = sd.InputStream(
        samplerate=_SAMPLE_RATE,
        blocksize=_BLOCK_SIZE,
        channels=_CHANNELS,
        callback=callback,
        dtype='int16',
    )
    stream.start()
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        stream.stop()
        stream.close()
        return

async def _mic_consume_chunk(queue: asyncio.Queue, call_id: str, turn_id: str):

    while True:
        try:
            chunk = await queue.get()
        except asyncio.QueueEmpty:
            print("Queue is empty")
            break
        async for audio_chunk in _chunk_pcm(chunk.tobytes(), call_id=call_id, turn_id=turn_id):
            yield audio_chunk
        queue.task_done()
    

async def mic_consumer(queue: asyncio.Queue):
    asr_provider = ASRProvider()
    call_id = "123"
    turn_id = "456"
    audio_stream = _mic_consume_chunk(queue, call_id=call_id, turn_id=turn_id)
    try:
        while True:
            async for transcript in asr_provider.transcribe_audio(audio_stream, call_id, turn_id):
                print("mic consumer got transcript")
                print(transcript)
                yield transcript
                
        queue.task_done()

    except asyncio.CancelledError:
        return


# async def _text_to_speech_consume_chunk(queue: asyncio.Queue, call_id: str, turn_id: str):
#     while True:
#         try:
#             text = await mic_consumer(queue)
#             print("text to speech consumer got text")
#             print(text)
#         except asyncio.QueueEmpty:
#             print("Queue is empty")
#             break
       
#             yield text
#         queue.task_done()

async def text_to_speech_consumer(queue: asyncio.Queue, transcript_stream: AsyncIterator[PartialTranscript | FullTranscript]):
    tts_provider = TTSProvider()

    call_id = "123"
    turn_id = "456"
    print("text to speech consumer started")
    async def extract_text():
        async for transcript in transcript_stream:
            if isinstance(transcript, FullTranscript):
                yield transcript.text
            # Use .text or whatever attribute holds the string on your Transcript model
            
    try:
        while True:
            print("text to speech consumer waiting for transcript")
            async for audio_chunk in tts_provider.synthesize_audio(extract_text(), call_id, turn_id):
                print("synthesizing audio chunk")
                print(len(audio_chunk))
                queue.put_nowait(audio_chunk)
        queue.task_done()
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(e)
        return

async def speech_streamer(speaker_queue: std_queue.Queue):

    def callback(outdata, frames, time, status):
        try:
            # 1. Safely pull from the standard thread-safe queue
            chunk = speaker_queue.get_nowait()
            
            # 2. Write bytes to outdata, safely handling size mismatches
            length = min(len(chunk), len(outdata))
            outdata[:length] = chunk[:length]
            
            # 3. Pad with silence if the chunk is smaller than the required frame size
            if length < len(outdata):
                outdata[length:] = b'\x00' * (len(outdata) - length)
                
        except std_queue.Empty:
            # Play silence if the TTS hasn't provided audio yet
            outdata[:] = b'\x00' * len(outdata)

    speaker_stream = sd.RawOutputStream(
        samplerate=_SAMPLE_RATE,
        channels=_CHANNELS,
        dtype='int16',
        blocksize=0,
        callback=callback,
    )
    speaker_stream.start()
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        speaker_stream.stop()
        speaker_stream.close()
        return

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
        self.text_to_speech_queue: asyncio.Queue[str] = asyncio.Queue()

    async def get_audio_files(self, call_id: str, turn_id: str) -> AsyncIterator[AsyncIterator[ASRBytes]]:
        audio_files_path = Path(__file__).resolve().parent.parent / "audio_db" / "audio_files"
        for audio_file in sorted(audio_files_path.iterdir()):
            if not audio_file.is_file() or audio_file.suffix.lower() != ".wav":
                continue
            pcm = _wav_to_pcm16k_mono(audio_file.read_bytes())
            yield _chunk_pcm(pcm, call_id=call_id, turn_id=turn_id)

    async def speech_to_text_orchestrate(
        self, call_id: str, turn_id: str
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        async for audio_chunks in self.get_audio_files(call_id, turn_id):
            async for transcript in self.asr_provider.transcribe_audio(audio_chunks, call_id, turn_id):
                yield transcript

    async def text_to_speech_orchestrate(
        self, text: str, call_id: str, turn_id: str
    ) -> AsyncIterator[bytes]:
        await self.text_to_speech_queue.put(text)

        async def tokens() -> AsyncIterator[str]:
            yield await self.text_to_speech_queue.get()

        async for audio_chunk in self.tts_provider.synthesize_audio(tokens(), call_id, turn_id):
            yield audio_chunk


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
    async def main():
        init_engine(os.getenv("DATABASE_URL"))
        loop = asyncio.get_event_loop()
        mic_queue = asyncio.Queue()
        speaker_queue = std_queue.Queue()
        stream_task = asyncio.create_task(mic_streamer(mic_queue, loop))
        print("stream task started")
        transcript_stream = mic_consumer(mic_queue)
        print("transcript stream started")
        text_to_speech_task = asyncio.create_task(text_to_speech_consumer(speaker_queue, transcript_stream))
        print("text to speech task started")
        speech_streamer_task = asyncio.create_task(speech_streamer(speaker_queue))
        try:
            await asyncio.gather(stream_task, text_to_speech_task, speech_streamer_task)
        except asyncio.CancelledError:
            stream_task.cancel()
            text_to_speech_task.cancel()
            speech_streamer_task.cancel()   
            await asyncio.gather(stream_task, text_to_speech_task, speech_streamer_task, return_exceptions=True)
            loop.stop()
            loop.close()
        except Exception as e:
            print(e)
    asyncio.run(main())
