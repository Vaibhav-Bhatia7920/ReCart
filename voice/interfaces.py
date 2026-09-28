from typing import Protocol, AsyncIterator
from voice.template import PartialTranscript, FullTranscript, ASRBytes

from voice.adapters.deepgram_asr import DeepgramASR
from voice.adapters.deepgram_tts import DeepgramTTS
import os
from dotenv import load_dotenv
load_dotenv()

DEEPGRAM_API_KEY = os.getenv("DEEPGRAM_API_KEY")

class ASRProvider(Protocol):
    def __init__(self):
        self.transciber = DeepgramASR(DEEPGRAM_API_KEY)

    async def transcribe_audio(
        self, audio_chunks: AsyncIterator[ASRBytes], call_id: str, turn_id: str
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        """
        Streams audio chunks in; yields PartialTranscript objects as
        interim (possibly-revised) results arrive, and exactly one
        FullTranscript per utterance when the provider signals it as final.
        Stream ends cleanly on end-of-utterance; a dropped connection must
        raise ASRProviderError (see voice/errors.py) rather than silently
        stopping iteration.
        """
        transciber = self.transciber
        async for result in transciber.transcribe_audio(audio_chunks, call_id, turn_id):
            yield result
            


class TTSProvider(Protocol):
    def __init__(self):
        self.synthesizer = DeepgramTTS(DEEPGRAM_API_KEY)

    async def synthesize_audio(
        self, text: AsyncIterator[str], call_id: str, turn_id: str
    ) -> AsyncIterator[bytes]:
        """
        Streams text chunks in (token/sentence pieces); yields raw audio
        bytes as they're generated. Stream ends cleanly when generation
        completes; a dropped connection must raise TTSProviderError.
        """
        synthesizer = self.synthesizer
        async for result in synthesizer.synthesize_audio(text, call_id, turn_id):
            yield result