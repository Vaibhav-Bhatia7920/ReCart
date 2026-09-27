from collections.abc import AsyncIterator
from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent.errors import UnclassifiedIntentError
from agent.graph import build_turn_graph, route_after_intent, run_turn
from agent.intents import ALWAYS_ESCALATE, Intent, IntentClassification
from agent.state import TurnOutcome, TurnState
from state.call_state import cleanup_test_call, commit_turn, start_call
from state.schemas import ToolCallRecord, TurnLogInput
from telemetry.instrument import TelemetryInstrument
from voice.template import ASR_Bytes

MEDICINE = {
    "name": "Amoxicillin 500mg",
    "description": "Penicillin antibiotic used for bacterial infections.",
    "use_cases": ["sinus infection", "strep throat"],
    "warnings": ["Do not use if allergic to penicillin"],
    "sku": "AMOX-500",
}


async def _empty_audio() -> AsyncIterator[ASR_Bytes]:
    if False:
        yield ASR_Bytes(bytes=b"", timestamp=0.0, call_id="", turn_id="")


class RecordingInstrument(TelemetryInstrument):
    def __init__(self) -> None:
        super().__init__("intent_classification", "intent_classification", _empty_audio())
        self.events: list[str] = []

    async def mark_event(self, event: str) -> None:
        self.events.append(event)


class ScriptedClassifier:
    def __init__(self, intent: Intent) -> None:
        self.intent = intent

    async def classify(self, state: TurnState) -> IntentClassification:
        del state
        return IntentClassification(intent=self.intent)


async def _start(call_id: str) -> None:
    await start_call(
        call_id,
        customer_id="cust-1",
        phone_number="+15551212",
        user_segment="frequent_buyer",
        initial_cart_snapshot=[MEDICINE],
    )


def _log(turn_id: int) -> TurnLogInput:
    return TurnLogInput(
        turn_id=turn_id,
        user_raw_transcript=f"utterance-{turn_id}",
        classified_intent="other",
        tool_calls=[],
        agent_response_text=f"reply-{turn_id}",
    )


def test_intent_schema_rejects_unknown_label() -> None:
    with pytest.raises(ValidationError):
        IntentClassification.model_validate({"intent": "maybe_clinical"})
    assert ALWAYS_ESCALATE == frozenset({Intent.CLINICAL_QUESTION})


def test_route_after_intent_is_a_hard_edge() -> None:
    clinical = TurnState(
        call_id="c",
        turn_id=1,
        user_transcript="side effects?",
        classified_intent=Intent.CLINICAL_QUESTION,
    )
    discount = TurnState(call_id="c", turn_id=1, user_transcript="any discount?", classified_intent=Intent.ASK_DISCOUNT)
    missing = TurnState(call_id="c", turn_id=1, user_transcript="hello")
    assert route_after_intent(clinical) == "escalate"
    assert route_after_intent(discount) == "decide_action"
    with pytest.raises(UnclassifiedIntentError):
        route_after_intent(missing)


async def test_load_call_context_keeps_last_two_turns(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    instrument = RecordingInstrument()
    await _start(call_id)
    try:
        for index in range(3):
            await commit_turn(call_id, index, _log(index + 1), {})
        graph = build_turn_graph(ScriptedClassifier(Intent.OTHER), instrument=instrument)
        result = await run_turn(
            graph,
            TurnState(
                call_id=call_id,
                turn_id=4,
                user_transcript="sounds good",
                tool_calls_this_turn=[
                    ToolCallRecord(tool="stale", args={}, idempotency_key="old"),
                ],
            ),
        )
        assert result.call_facts is not None
        assert result.call_facts.customer_id == "cust-1"
        assert [turn.turn_id for turn in result.recent_turns] == [2, 3]
        assert result.tool_calls_this_turn == []
        assert result.classified_intent is Intent.OTHER
        assert result.turn_outcome is TurnOutcome.CONTINUING
        assert instrument.events == ["intent_classification"]
    finally:
        await cleanup_test_call(call_id)


async def test_clinical_question_skips_decide_action(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    await _start(call_id)
    try:
        graph = build_turn_graph(ScriptedClassifier(Intent.CLINICAL_QUESTION), instrument=RecordingInstrument())
        nodes: list[str] = []
        async for update in graph.astream(
            TurnState(call_id=call_id, turn_id=1, user_transcript="can I take this with ibuprofen?"),
            stream_mode="updates",
        ):
            nodes.extend(update.keys())
        assert nodes == ["load_call_context", "intent_classification", "escalate"]
        result = await run_turn(
            graph,
            TurnState(call_id=call_id, turn_id=1, user_transcript="can I take this with ibuprofen?"),
        )
        assert result.turn_outcome is TurnOutcome.ESCALATED
        assert result.classified_intent is Intent.CLINICAL_QUESTION
    finally:
        await cleanup_test_call(call_id)
