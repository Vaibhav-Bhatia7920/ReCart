import asyncio
import io
import sys
import traceback
from collections.abc import AsyncIterator, Callable, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
import queue as std_queue
from voice.interfaces import ASRProvider, TTSProvider
from voice.template import ASRBytes, FullTranscript, PartialTranscript
# from app.db import init_engine  # Orchestrator CLI path: no Postgres/Redis yet (app container still uses DB).
from agent.simple_agent_stateless_v1 import SimpleAgentStatelessV1
from telemetry.helper_functions import log_event
from telemetry.logger import Logger
from orchestrator.helper_functions import _wav_to_pcm16k_mono, _mp3_to_pcm16k_mono, _chunk_pcm


_SAMPLE_RATE = 16000
_BLOCK_SIZE = 1024
_CHANNELS = 1
from dotenv import load_dotenv

load_dotenv()


class _MicStreamStop:
    """Sentinel: end live mic PCM stream without cancelling the async generator."""


_MIC_STOP = _MicStreamStop()


def _log_pipeline_error(step: str, exc: BaseException) -> None:
    print(f"[speaker_orchestrate] {step} failed: {exc!r}", file=sys.stderr, flush=True)
    traceback.print_exception(type(exc), exc, exc.__traceback__, file=sys.stderr)


MicStreamerFn = Callable[
    [asyncio.Queue, asyncio.AbstractEventLoop, str, str, Logger],
    Coroutine[Any, Any, None],
]
SpeechStreamerFn = Callable[[std_queue.Queue], Coroutine[Any, Any, None]]
AgentFlowFn = Callable[
    [AsyncIterator[PartialTranscript | FullTranscript], Logger],
    AsyncIterator[PartialTranscript | FullTranscript],
]


async def mic_streamer(
    queue: asyncio.Queue,
    loop: asyncio.AbstractEventLoop,
    call_id: str,
    turn_id: str,
    logger: Logger,
):
    import sounddevice as sd

    def callback(indata, frames, time, status):
        audio_chunk = indata.copy()
        loop.call_soon_threadsafe(queue.put_nowait, audio_chunk)

    logger.log_event("Mic Streamer Started", call_id, turn_id)
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

async def _mic_live_pcm_stream(
    mic_queue: asyncio.Queue,
    call_id: str,
    turn_id: str,
) -> AsyncIterator[ASRBytes | bytes]:
    """16 kHz int16 mic blocks → chunked PCM for live ASR (same shape as voice_ws ``_pcm_stream``)."""
    try:
        while True:
            chunk = await mic_queue.get()
            if isinstance(chunk, _MicStreamStop):
                mic_queue.task_done()
                return
            pcm = chunk.tobytes() if hasattr(chunk, "tobytes") else bytes(chunk)
            async for audio_chunk in _chunk_pcm(pcm, call_id=call_id, turn_id=turn_id):
                yield audio_chunk
            mic_queue.task_done()
    except asyncio.CancelledError:
        raise


async def _signal_mic_stop(mic_queue: asyncio.Queue) -> None:
    await mic_queue.put(_MIC_STOP)


async def _drain_live_stt(
    speech_to_text_stream: AsyncIterator[PartialTranscript | FullTranscript],
    *,
    timeout: float = 3.0,
) -> None:
    """Pull until Deepgram closes after mic stop (avoids async-generator aclose while running)."""
    try:
        async with asyncio.timeout(timeout):
            async for _transcript in speech_to_text_stream:
                del _transcript
    except TimeoutError:
        print("[speaker_orchestrate] STT drain timed out", file=sys.stderr, flush=True)
    except StopAsyncIteration:
        return
    except Exception as exc:
        _log_pipeline_error("STT drain", exc)


async def agent_flow(transcript_stream: AsyncIterator[PartialTranscript | FullTranscript], logger: Logger):
    call_id = "123"
    turn_id = "456"
    first_full_transcript = False
    
    agent = SimpleAgentStatelessV1()
    transcript_iter = transcript_stream.__aiter__()
    try:
        print("agent flow started")
        while True:
            try:
                transcript = await transcript_iter.__anext__()
            except StopAsyncIteration:
                return
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
                return
    except asyncio.CancelledError:
        raise
    finally:
        logger.log_event("Last Agent Transcript", call_id, turn_id, "nova-3")


async def speech_streamer(speaker_queue: std_queue.Queue):
    import sounddevice as sd

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
            raise

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
            raise

    async def tts_local_consumer(self, audio_stream: AsyncIterator[bytes]):
        try:
            async for audio_chunk in audio_stream:
                print(audio_chunk)
        except asyncio.CancelledError:
            return


async def _tts_speaker_consumer(
    audio_stream: AsyncIterator[bytes],
    speaker_queue: std_queue.Queue,
) -> None:
    """Play TTS PCM through ``speech_streamer`` (local stand-in for voice_ws byte sends)."""
    async for audio_chunk in audio_stream:
        if not audio_chunk:
            continue
        speaker_queue.put_nowait(audio_chunk)


async def _run_stt_agent_tts_pipeline(
    orchestrator: DeepgramOrchestrator,
    speech_to_text_stream: AsyncIterator[PartialTranscript | FullTranscript],
    agent_flow_fn: AgentFlowFn,
    logger: Logger,
    call_id: str,
    turn_id: str,
    speaker_queue: std_queue.Queue,
) -> None:
    agent_stream = agent_flow_fn(speech_to_text_stream, logger)
    text_to_speech_stream = orchestrator.agent_to_tts_orchestrate(
        agent_stream, call_id, turn_id, logger
    )
    await _tts_speaker_consumer(text_to_speech_stream, speaker_queue)


async def _cancel_background_tasks(*tasks: asyncio.Task) -> None:
    pending = [task for task in tasks if not task.done()]
    for task in pending:
        task.cancel()
    if pending:
        await asyncio.gather(*pending, return_exceptions=True)


async def _wait_for_speaker_queue_drain(
    speaker_queue: std_queue.Queue,
    *,
    timeout: float = 5.0,
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not speaker_queue.empty() and loop.time() < deadline:
        await asyncio.sleep(0.01)


@dataclass(frozen=True)
class SpeakerOrchestrateDeps:
    """Injected I/O and pipeline steps (defaults = real mic/speaker + agent)."""

    mic_streamer: MicStreamerFn | None = None
    speech_streamer: SpeechStreamerFn | None = None
    agent_flow: AgentFlowFn | None = None
    asr: Any | None = None
    tts: Any | None = None
    max_turns: int | None = None  # None = loop until Ctrl+C; tests set 1


async def speaker_orchestrate(
    call_id: str,
    turn_id: str,
    logger: Logger,
    deps: SpeakerOrchestrateDeps | None = None,
) -> None:
    """Local end-to-end turn: mic → ASR → agent → TTS → speakers (mirrors ``database_orchestrate`` / voice_ws)."""
    options = deps or SpeakerOrchestrateDeps()
    # if os.getenv("DATABASE_URL"):
    #     init_engine(os.getenv("DATABASE_URL"))

    mic_streamer_fn = options.mic_streamer or mic_streamer
    speech_streamer_fn = options.speech_streamer or speech_streamer
    agent_flow_fn = options.agent_flow or agent_flow
    asr = options.asr if options.asr is not None else ASRProvider()
    tts = options.tts if options.tts is not None else TTSProvider()

    loop = asyncio.get_running_loop()
    mic_queue: asyncio.Queue = asyncio.Queue()
    speaker_queue: std_queue.Queue = std_queue.Queue()
    orchestrator = DeepgramOrchestrator(asr, tts)

    mic_task = asyncio.create_task(
        mic_streamer_fn(mic_queue, loop, call_id, turn_id, logger),
    )
    speaker_task = asyncio.create_task(speech_streamer_fn(speaker_queue))

    async def run_turn(active_turn_id: str) -> None:
        speech_to_text_stream: AsyncIterator[PartialTranscript | FullTranscript] | None = None
        try:
            speech_to_text_stream = orchestrator.speech_to_text_live(
                _mic_live_pcm_stream(mic_queue, call_id, active_turn_id),
                call_id,
                active_turn_id,
                logger,
            )
            await _run_stt_agent_tts_pipeline(
                orchestrator,
                speech_to_text_stream,
                agent_flow_fn,
                logger,
                call_id,
                active_turn_id,
                speaker_queue,
            )
        except Exception as exc:
            _log_pipeline_error(f"turn {active_turn_id}", exc)
            raise
        finally:
            try:
                await _wait_for_speaker_queue_drain(speaker_queue, timeout=60.0)
                await asyncio.sleep(0.25)
            except Exception as exc:
                _log_pipeline_error("speaker playback drain", exc)
            orchestrator.stop_event.set()
            try:
                await _signal_mic_stop(mic_queue)
            except Exception as exc:
                _log_pipeline_error("mic stop", exc)
            if speech_to_text_stream is not None:
                await _drain_live_stt(speech_to_text_stream, timeout=2.0)

    turn_index = 1
    try:
        while True:
            orchestrator.reset_interrupt()
            active_turn_id = turn_id if turn_index == 1 else f"{turn_id}-{turn_index}"
            print(
                f"[speaker_orchestrate] turn {turn_index}: listening…",
                flush=True,
            )
            pipeline_task = asyncio.create_task(run_turn(active_turn_id))
            orchestrator._current_tts_task = pipeline_task
            await pipeline_task
            print(
                f"[speaker_orchestrate] turn {turn_index} complete — speak again (Ctrl+C to quit)",
                flush=True,
            )
            turn_index += 1
            if options.max_turns is not None and turn_index > options.max_turns:
                break
    except asyncio.CancelledError:
        raise
    except KeyboardInterrupt:
        print("[speaker_orchestrate] stopped by user", flush=True)
    finally:
        try:
            await _signal_mic_stop(mic_queue)
            await _cancel_background_tasks(mic_task, speaker_task)
            await asyncio.sleep(0.05)
        except Exception as exc:
            _log_pipeline_error("speaker_orchestrate shutdown", exc)

async def database_orchestrate(call_id: str, turn_id: str, path: Path, logger: Logger):
    # init_engine(os.getenv("DATABASE_URL"))
    print("database orchestrate started")
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
    orchestrator._current_tts_task = final_audio_stream
    try:
        await final_audio_stream
        print("database orchestrate finished")
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        print(f"database_orchestrate failed: {exc!r}", flush=True)
        raise


if __name__ == "__main__":
    async def main():
        user_input = input("Speaker or Database: ")
        call_id = "123"
        turn_id = "456"
        path = Path(__file__).parent.parent / "audio_db" / "audio_files" / "human-nonhuman" / "human"
        logger = Logger(call_id)
        if user_input == "Speaker":
            await speaker_orchestrate(call_id, turn_id, logger)
        elif user_input == "Database":
            await database_orchestrate(call_id, turn_id, path, logger)
        else:
            print("Invalid input")
            return
        
    try:
        asyncio.run(main())
    except Exception as exc:
        print(f"orchestrator exited with error: {exc!r}", flush=True)
        raise SystemExit(1) from exc
