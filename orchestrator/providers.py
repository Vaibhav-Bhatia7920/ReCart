import asyncio
import audioop
import io
import time
import wave
from collections.abc import AsyncIterator
from pathlib import Path

import io
import os
import numpy as np
import sounddevice as sd
import queue as std_queue
from voice.interfaces import ASRProvider, TTSProvider
from voice.template import ASRBytes, FullTranscript, PartialTranscript
from app.db import init_engine
from agent.simple_agent_stateless_v1 import SimpleAgentStatelessV1

_PCM_RATE = 16000
_CHUNK_BYTES = 3200  # 100 ms of 16 kHz 16-bit mono
_SAMPLE_RATE = 16000
_BLOCK_SIZE = 1024
_CHANNELS = 1
from dotenv import load_dotenv

load_dotenv()

async def mic_streamer(queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
    print("mic streamer started")
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
    print("mic consumer started")
    while True:
        try:
            chunk = await queue.get()
            print("mic consumer got chunk")
            
        except asyncio.QueueEmpty:
            print("Queue is empty")
            print("mic consumer got queue empty")
            break
        async for audio_chunk in _chunk_pcm(chunk.tobytes(), call_id=call_id, turn_id=turn_id):
            print("mic consumer got audio chunk")
            
            yield audio_chunk
        queue.task_done()
    

async def mic_consumer(queue: asyncio.Queue):
    asr_provider = ASRProvider()
    call_id = "123"
    turn_id = "456"
    audio_stream = _mic_consume_chunk(queue, call_id=call_id, turn_id=turn_id)
    print("mic consumer audio stream started")
    try:  
        async for transcript in asr_provider.transcribe_audio(audio_stream, call_id, turn_id):
            print("mic consumer got transcript")
            print(transcript)
            yield transcript
            
        # queue.task_done()

    except asyncio.CancelledError:
        return

async def agent_flow(transcript_stream: AsyncIterator[PartialTranscript | FullTranscript]):
    try:
        agent = SimpleAgentStatelessV1()
    except Exception as e:
        print(e)
        return
    try:
        print("agent flow started")
        async for transcript in transcript_stream:
            print("agent flow got transcript")
            if isinstance(transcript, FullTranscript):
                response = await agent.run(transcript.text)
                transcript.text = response
                print("agent flow got response")
                print(response)
                yield transcript
    except asyncio.CancelledError:
        return
    except Exception as e:
        print(e)
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
        print("text to speech consumer extract text started")
        async for transcript in transcript_stream:

            print("text to speech consumer got transcript")
            if isinstance(transcript, FullTranscript):
                yield transcript.text
            # Use .text or whatever attribute holds the string on your Transcript model
            
    try:
        async for audio_chunk in tts_provider.synthesize_audio(extract_text(), call_id, turn_id):
            print("text to speech consumer got audio chunk")
            if not audio_chunk:
                continue
            
            # ─── DEBUG PRINTS START ───
            print("--- AUDIO CHUNK RECEIVED ---")
            print(f"Byte Length: {len(audio_chunk)}")
            # Print the first 20 bytes as a string to check for WAV/RIFF headers
            print(f"First 20 bytes (Text): {audio_chunk[:20]}")
            # Print hex format to check structure
            print(f"First 10 bytes (Hex): {audio_chunk[:10].hex()}")
            # ─── DEBUG PRINTS END ───
            
            queue.put_nowait(audio_chunk)
        # queue.task_done()
    except asyncio.CancelledError:
        print("text to speech consumer cancelled")
        return
    except Exception as e:
        print(e)
        print("text to speech consumer exception")
        return

async def speech_streamer(speaker_queue: std_queue.Queue):

    audio_buffer = io.BytesIO()

    def callback(outdata, frames, time, status):
        # 16-bit Mono = 2 bytes per frame
        expected_bytes = frames * 2 
        
        # 1. Pull any new incoming audio bytes from the queue and append them to our stream buffer
        while True:
            try:
                chunk = speaker_queue.get_nowait()
                # Write to the end of our current buffer stream
                current_pos = audio_buffer.tell()
                audio_buffer.seek(0, io.SEEK_END)
                audio_buffer.write(chunk)
                audio_buffer.seek(current_pos)
            except std_queue.Empty:
                break

        # Get a predictable memory view wrapper over outdata
        outdata_bytes = memoryview(outdata)

        # 2. Read exactly what the speaker needs from the current playback window
        playback_chunk = audio_buffer.read(expected_bytes)
        read_len = len(playback_chunk)

        if read_len > 0:
            outdata_bytes[:read_len] = playback_chunk
            
            # Pad with zeroed silence if we ran out of data mid-buffer
            if read_len < expected_bytes:
                outdata_bytes[read_len:] = b'\x00' * (expected_bytes - read_len)
        else:
            # FIX: Clear the memory block safely via slice assignment
            outdata_bytes[:] = b'\x00' * expected_bytes

        # 3. Memory Cleanup: Clear out spent bytes from the top of the stream to save RAM
        if audio_buffer.tell() > 100_000:  # Clean every ~100KB consumed
            remaining_data = audio_buffer.read()
            audio_buffer.seek(0)
            audio_buffer.truncate(0)
            audio_buffer.write(remaining_data)
            audio_buffer.seek(0)

    speaker_stream = sd.RawOutputStream(
        samplerate=24000,
        channels=1,
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
        agent_stream = agent_flow(transcript_stream)
        print("agent stream started")
        text_to_speech_task = asyncio.create_task(text_to_speech_consumer(speaker_queue, agent_stream))
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
