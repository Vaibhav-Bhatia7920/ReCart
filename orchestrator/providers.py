import asyncio
import audioop
import io
import time
import wave
from collections.abc import AsyncIterator
from pathlib import Path
import asyncio
from pathlib import Path
from typing import AsyncIterator, Optional
import subprocess
import io
import os
import numpy as np
import sounddevice as sd
import queue as std_queue
from voice.interfaces import ASRProvider, TTSProvider
from voice.template import ASRBytes, FullTranscript, PartialTranscript
from app.db import init_engine
from agent.simple_agent_stateless_v1 import SimpleAgentStatelessV1
from telemetry.helper_functions import log_event
from telemetry.logger import Logger
from orchestrator.helper_functions import _wav_to_pcm16k_mono, _mp3_to_pcm16k_mono, _chunk_pcm


_PCM_RATE = 16000
_CHUNK_BYTES = 3200  # 100 ms of 16 kHz 16-bit mono
_SAMPLE_RATE = 16000
_BLOCK_SIZE = 1024
_CHANNELS = 1
from dotenv import load_dotenv

load_dotenv()

async def mic_streamer(queue: asyncio.Queue, loop: asyncio.AbstractEventLoop):
    print("mic streamer started")
    call_id = "123"
    turn_id = "456"
    def callback(indata, frames, time, status):
        audio_chunk = indata.copy()
        loop.call_soon_threadsafe(queue.put_nowait, audio_chunk)

    logger.info("Mic Streamer Started", extra={"extra_fields": {"call_id": call_id, "turn_id": turn_id}})
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
        logger.info("Mic Consumer Started", extra={"extra_fields": {"call_id": call_id, "turn_id": turn_id, "model": "silero-vad"}})
        async for transcript in asr_provider.transcribe_audio(audio_stream, call_id, turn_id):
            print("mic consumer got transcript")
            print(transcript)
            if isinstance(transcript, FullTranscript):
                yield transcript
            
            
        # queue.task_done()

    except asyncio.CancelledError:
        return


async def agent_flow(transcript_stream: AsyncIterator[PartialTranscript | FullTranscript], logger: Logger):
    call_id = "123"
    turn_id = "456"
    first_full_transcript = False
    
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
                if not first_full_transcript:
                    first_full_transcript = True
                    logger.log_event("First Agent Transcript", call_id, turn_id, "nova-3")
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
    finally:
        logger.log_event("Last Agent Transcript", call_id, turn_id, "nova-3")
        

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
    call_id = "123"
    turn_id = "456"

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

    log_event(call_id, "Speech Streamer Started", turn_id)
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



async def _one_text(text: str) -> AsyncIterator[str]:
    yield text


class DeepgramOrchestrator:
    def __init__(
        self,
        asr_provider: ASRProvider,
        tts_provider: TTSProvider,
        path: Path | None = None,
    ):
        self.asr_provider = asr_provider
        self.tts_provider = tts_provider
        self.text_to_speech_queue: asyncio.Queue[str] = asyncio.Queue()
        self.path = path
        
        # --- Barge-in state management ---
        self.stop_event = asyncio.Event()
        self._current_tts_task: Optional[asyncio.Task] = None

    def interrupt(self):
        """Call this immediately when Silero VAD detects Speech STARTED."""
        print("🚨 Interrupting: Barge-in detected!")
        # 1. Signal any running loops to abort
        self.stop_event.set()

        if self._current_tts_task and not self._current_tts_task.done():
            self._current_tts_task.cancel()

       
        while not self.text_to_speech_queue.empty():
            try:
                self.text_to_speech_queue.get_nowait()
                self.text_to_speech_queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break

    def reset_interrupt(self):
        """Call this before starting a new turn."""
        self.stop_event.clear()

    async def get_audio_files(self, call_id: str, turn_id: str) -> AsyncIterator[AsyncIterator[ASRBytes]]:
        if self.path is None:
            raise ValueError("DeepgramOrchestrator.path is required for file-based ASR")
        audio_files_path = self.path
        for audio_file in sorted((audio_files_path.iterdir()))[:1]:
            if not audio_file.is_file() or audio_file.suffix.lower() not in [".wav", ".mp3"]:
                continue
            if audio_file.suffix.lower() == ".mp3":
                pcm = _mp3_to_pcm16k_mono(audio_file.read_bytes())
            else:   
                pcm = _wav_to_pcm16k_mono(audio_file.read_bytes())
            yield _chunk_pcm(pcm, call_id=call_id, turn_id=turn_id)

    async def speech_to_text_orchestrate(
        self, call_id: str, turn_id: str, logger: Logger
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        try:
            async for audio_chunks in self.get_audio_files(call_id, turn_id):
                async for transcript in self.asr_provider.transcribe_audio(audio_chunks, call_id, turn_id):
                    # Exit immediately if interrupted
                    if self.stop_event.is_set():
                        return
                    yield transcript
        except asyncio.CancelledError:
            return

    async def speech_to_text_live(
        self,
        audio_chunks: AsyncIterator[ASRBytes | bytes],
        call_id: str,
        turn_id: str,
        logger: Logger,
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        """Realtime ASR from a live PCM stream (WebSocket / mic), same yield shape as file STT."""
        del logger  # parity with speech_to_text_orchestrate signature
        try:
            async for transcript in self.asr_provider.transcribe_audio(audio_chunks, call_id, turn_id):
                if self.stop_event.is_set():
                    return
                yield transcript
        except asyncio.CancelledError:
            return

    async def agent_to_tts_orchestrate(
        self,
        transcript_stream: AsyncIterator[PartialTranscript | FullTranscript], 
        call_id: str,
        turn_id: str,
        logger: Logger
    ) -> AsyncIterator[bytes]:
        del logger
        try:
            async for transcript in transcript_stream:
                if self.stop_event.is_set():
                    return
                if isinstance(transcript, FullTranscript):
                    await self.text_to_speech_queue.put(transcript.text)
                    break

            if self.stop_event.is_set() or self.text_to_speech_queue.empty():
                return

            text_to_synthesize = await self.text_to_speech_queue.get()

            async for audio_chunk in self.tts_provider.synthesize_audio(
                _one_text(text_to_synthesize), call_id, turn_id
            ):
                if self.stop_event.is_set():
                    return
                yield audio_chunk

        except asyncio.CancelledError:
           
            return
    
    async def tts_local_consumer(self, audio_stream: AsyncIterator[bytes]):
      
        try:
            async for audio_chunk in audio_stream:
                print(audio_chunk)
        except asyncio.CancelledError:
            return




async def speaker_orchestrate(call_id: str, turn_id: str, logger: Logger):
    loop = asyncio.get_event_loop()
    mic_queue = asyncio.Queue()
    speaker_queue = std_queue.Queue()
    orchestrator = DeepgramOrchestrator(ASRProvider(), TTSProvider())
    with logger.span_event("Speech to Text", call_id, turn_id):
        speech_to_text_stream = orchestrator.speech_to_text_orchestrate(call_id, turn_id)
    with logger.span_event("Agent Flow ", call_id, turn_id):
        agent_stream = agent_flow(speech_to_text_stream)
    with logger.span_event("Text to Speech", call_id, turn_id):
        text_to_speech_stream = orchestrator.text_to_speech_orchestrate(call_id, turn_id)
    with logger.span_event("Speech Streamer Started", call_id, turn_id):
        speech_streamer_task = asyncio.create_task(speech_streamer(speaker_queue))
    try:
        await asyncio.gather(speech_to_text_stream, text_to_speech_stream, speech_streamer_task)
    except asyncio.CancelledError:
        return

async def database_orchestrate(call_id: str, turn_id: str, path: Path, logger: Logger):
    init_engine(os.getenv("DATABASE_URL"))
    loop = asyncio.get_event_loop()
    mic_queue = asyncio.Queue()
    print("database orchestrate started")
    speaker_queue = std_queue.Queue()
    orchestrator = DeepgramOrchestrator(ASRProvider(), TTSProvider(), path)
    
    print("speech to text orchestrate started")
    try:
        print("speech to text orchestrate started 2")
        speech_to_text_stream = orchestrator.speech_to_text_orchestrate(call_id, turn_id, logger)
    except Exception as e:
        print(e)
        raise e
   
    print("agent flow orchestrate started")
    agent_stream = (agent_flow(speech_to_text_stream, logger))

    print("text to speech orchestrate started")
    text_to_speech_stream = (orchestrator.agent_to_tts_orchestrate(agent_stream, call_id, turn_id, logger))
    final_audio_stream = asyncio.create_task(orchestrator.tts_local_consumer(text_to_speech_stream))
    orchestrator.current_tts_task = final_audio_stream
    try:
        await asyncio.gather(final_audio_stream)
        print("database orchestrate finished")
    except Exception as e:
        print(e)
        return
    except asyncio.CancelledError:
        return


if __name__ == "__main__":
    async def main():
        user_input = input("Speaker or Database: ")
        call_id = "123"
        turn_id = "456"
        logger = Logger(call_id, turn_id)
        if user_input == "Speaker":
            await speaker_orchestrate(call_id, turn_id,logger)
        elif user_input == "Database":
            await database_orchestrate(call_id, turn_id, path, logger)
        else:
            print("Invalid input")
            return
        
    asyncio.run(main())
