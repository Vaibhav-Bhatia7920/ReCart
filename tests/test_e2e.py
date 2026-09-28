from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

from agent.graph import build_turn_graph, run_turn
from agent.intents import Intent, IntentClassification
from agent.respond import SpokenResponse
from agent.state import TurnOutcome, TurnState
from agent.tools import ActionDecision
from orchestrator.providers import DeepgramOrchestrator
from state.call_state import cleanup_test_call, start_call
from telemetry.helper_functions import fetch_call_events
from telemetry.instrument import TelemetryInstrument
from voice.template import ASRBytes, FullTranscript, PartialTranscript

MEDICINE = {
    "name": "Amoxicillin 500mg",
    "description": "Penicillin antibiotic used for bacterial infections.",
    "use_cases": ["sinus infection", "strep throat"],
    "warnings": ["Do not use if allergic to penicillin"],
    "sku": "AMOX-500",
}

_USER_UTTERANCE = "I still have this cart, can you help me check out?"
_AGENT_REPLY = "I can help you finish that order."


class PipelineASR:
    """Stands in for DeepgramASR on the orchestrator ASRProvider surface."""

    def __init__(self, text: str) -> None:
        self._text = text

    async def transcribe_audio(
        self,
        audio_chunks: AsyncIterator[ASRBytes],
        call_id: str,
        turn_id: str,
    ) -> AsyncIterator[PartialTranscript | FullTranscript]:
        async for _chunk in audio_chunks:
            pass
        telemetry = TelemetryInstrument("deepgram_asr", "pipeline asr", call_id, turn_id)
        await telemetry.mark_event("First Partial Transcript")
        yield PartialTranscript(
            call_id=call_id,
            turn_id=turn_id,
            text=self._text,
            timestamp=0.0,
            stability=0.4,
        )
        await telemetry.mark_event("Final Transcript")
        yield FullTranscript(
            call_id=call_id,
            turn_id=turn_id,
            text=self._text,
            end_time=1.0,
            confidence=0.99,
        )


class PipelineTTS:
    """Stands in for DeepgramTTS on the orchestrator TTSProvider surface."""

    def __init__(self) -> None:
        self.spoken: list[str] = []

    async def synthesize_audio(
        self,
        text: AsyncIterator[str],
        call_id: str,
        turn_id: str,
    ) -> AsyncIterator[bytes]:
        pieces: list[str] = []
        async for chunk in text:
            pieces.append(chunk)
        spoken = "".join(pieces)
        self.spoken.append(spoken)
        telemetry = TelemetryInstrument("deepgram_tts", "pipeline tts", call_id, turn_id)
        await telemetry.mark_event("First Audio Frame")
        yield spoken.encode("utf-8")
        await telemetry.mark_event("Flushed")


class ScriptedClassifier:
    async def classify(self, state: TurnState) -> IntentClassification:
        del state
        return IntentClassification(intent=Intent.OTHER)


class ScriptedPlanner:
    async def plan(self, state: TurnState) -> ActionDecision:
        del state
        return ActionDecision(tool_calls=[])


class ScriptedComposer:
    async def compose(self, state: TurnState) -> SpokenResponse:
        del state
        return SpokenResponse(agent_response_text=_AGENT_REPLY, current_turn_facts={"checkout": True})


async def test_speech_to_agent_to_speech_marks_telemetry(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    turn_id = "1"
    tts = PipelineTTS()
    orchestrator = DeepgramOrchestrator(PipelineASR(_USER_UTTERANCE), tts)
    await start_call(
        call_id,
        customer_id="cust-e2e",
        phone_number="+15550001",
        user_segment="frequent_buyer",
        initial_cart_snapshot=[MEDICINE],
    )
    try:
        user_transcript = ""
        async for transcript in orchestrator.speech_to_text_orchestrate(call_id, turn_id):
            if isinstance(transcript, FullTranscript):
                user_transcript = transcript.text
                break
        assert user_transcript == _USER_UTTERANCE

        graph = build_turn_graph(
            classifier=ScriptedClassifier(),
            planner=ScriptedPlanner(),
            composer=ScriptedComposer(),
        )
        turn = await run_turn(
            graph,
            TurnState(call_id=call_id, turn_id=1, user_transcript=user_transcript),
        )
        assert turn.turn_outcome is TurnOutcome.CONTINUING
        assert turn.agent_response_text == _AGENT_REPLY

        spoken_pcm: list[bytes] = []
        async for frame in orchestrator.text_to_speech_orchestrate(
            turn.agent_response_text, call_id, turn_id
        ):
            spoken_pcm.append(frame)
        assert tts.spoken == [_AGENT_REPLY]
        assert b"".join(spoken_pcm) == _AGENT_REPLY.encode("utf-8")

        events = await fetch_call_events(call_id, turn_id)
        marked = [item["event"] for item in events]
        assert marked[0] == "First Partial Transcript"
        assert marked[1] == "Final Transcript"
        assert marked[2] == {
            "turn_id": 1,
            "user_raw_transcript": _USER_UTTERANCE,
            "classified_intent": "other",
            "tool_calls": [],
            "agent_response_text": _AGENT_REPLY,
            "schema_version": 1,
        }
        assert marked[3] == "First Audio Frame"
        assert marked[4] == "Flushed"
    finally:
        await cleanup_test_call(call_id)
