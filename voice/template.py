from pydantic import BaseModel, Field


class VoiceTranscript(BaseModel):
    call_id: str
    turn_id: str

class PartialTranscript(VoiceTranscript):
    text: str
    timestamp: float
    stability: float = Field(
        ge=0.0,
        le=1.0,
        description="The stability of the transcript. 1.0 is the most stable, 0.0 is the least stable.",
    )

class FullTranscript(VoiceTranscript):
    text: str
    end_time: float
    confidence: float


class ASR_Bytes(BaseModel):
    bytes: bytes
    timestamp: float
    call_id: str
    turn_id: str


# Name used by voice.interfaces.ASRProvider.
ASRBytes = ASR_Bytes
