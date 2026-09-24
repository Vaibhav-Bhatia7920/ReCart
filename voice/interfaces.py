from typing import Protocol, AsyncIterator
from voice.template import PartialTranscript, FullTranscript, ASRBytes


class ASRProvider(Protocol):
    def transcribe_audio(
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
        ...


class TTSProvider(Protocol):
    def synthesize_audio(
        self, text: AsyncIterator[str], call_id: str, turn_id: str
    ) -> AsyncIterator[bytes]:
        """
        Streams text chunks in (token/sentence pieces); yields raw audio
        bytes as they're generated. Stream ends cleanly when generation
        completes; a dropped connection must raise TTSProviderError.
        """
        ...