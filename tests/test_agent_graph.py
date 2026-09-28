from typing import Any
from uuid import uuid4

import pytest
from pydantic import ValidationError

from agent.errors import UnclassifiedIntentError
from agent.graph import ESCALATION_RESPONSE, build_turn_graph, route_after_intent, run_turn
from agent.intents import ALWAYS_ESCALATE, Intent, IntentClassification
from agent.respond import SpokenResponse
from agent.state import TurnOutcome, TurnState
from agent.tools import TOOLS, ActionDecision, ToolCallChoice, ToolName
from state.call_state import cleanup_test_call, get_call_context, start_call
from state.call_state import commit_turn as persist_turn
from state.models import CallOutcome
from state.schemas import ToolCallRecord, TurnLogInput
from telemetry.instrument import TelemetryInstrument

MEDICINE = {
    "name": "Amoxicillin 500mg",
    "description": "Penicillin antibiotic used for bacterial infections.",
    "use_cases": ["sinus infection", "strep throat"],
    "warnings": ["Do not use if allergic to penicillin"],
    "sku": "AMOX-500",
}


class RecordingInstrument(TelemetryInstrument):
    def __init__(self) -> None:
        super().__init__("intent_classification", "intent_classification", call_id="", turn_id="")
        self.events: list[str | dict[str, Any]] = []

    async def mark_event(self, event: str | dict[str, Any]) -> None:
        self.events.append(event)


class ScriptedPlanner:
    def __init__(self, decision: ActionDecision) -> None:
        self.decision = decision

    async def plan(self, state: TurnState) -> ActionDecision:
        del state
        return self.decision


_NO_TOOLS = ActionDecision(tool_calls=[])
_REPLY = SpokenResponse(
    agent_response_text="Sounds good.",
    current_turn_facts={"confirmed": True},
)


class ScriptedComposer:
    def __init__(self, spoken: SpokenResponse) -> None:
        self.spoken = spoken

    async def compose(self, state: TurnState) -> SpokenResponse:
        del state
        return self.spoken


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


def test_tool_list_is_exactly_three() -> None:
    with pytest.raises(ValidationError):
        ToolCallChoice.model_validate({"tool": "cancel_order", "args": {}})
    assert TOOLS == (
        ToolName.APPLY_DISCOUNT,
        ToolName.GENERATE_FINAL_PAYMENT_LINK,
        ToolName.SEARCH_STORE,
    )


def test_route_after_intent_is_a_hard_edge() -> None:
    clinical = TurnState(
        call_id="c",
        turn_id=1,
        user_transcript="side effects?",
        classified_intent=Intent.CLINICAL_QUESTION,
    )
    discount = TurnState(call_id="c", turn_id=1, user_transcript="any discount?", classified_intent=Intent.ASK_DISCOUNT)
    vitamin = TurnState(
        call_id="c",
        turn_id=1,
        user_transcript="is this vitamin C okay daily?",
        classified_intent=Intent.OTHER,
    )
    missing = TurnState(call_id="c", turn_id=1, user_transcript="hello")
    assert route_after_intent(clinical) == "escalate"
    assert route_after_intent(discount) == "decide_action"
    assert route_after_intent(vitamin) == "decide_action"
    with pytest.raises(UnclassifiedIntentError):
        route_after_intent(missing)


async def test_load_call_context_keeps_last_two_turns(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    instrument = RecordingInstrument()
    await _start(call_id)
    try:
        for index in range(3):
            await persist_turn(call_id, index, _log(index + 1), {})
        graph = build_turn_graph(
            ScriptedClassifier(Intent.OTHER),
            instrument=instrument,
            planner=ScriptedPlanner(_NO_TOOLS),
            composer=ScriptedComposer(_REPLY),
        )
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
        assert result.agent_response_text == _REPLY.agent_response_text
        assert result.call_facts.call_outcome is CallOutcome.IN_PROGRESS
        assert result.call_facts.current_turn_index == 4
        assert result.call_facts.turn_facts == [{"confirmed": True}]
        stored = await get_call_context(call_id)
        assert [turn.turn_id for turn in stored.recent_turns] == [3, 4]
        assert instrument.events == [
            {
                "turn_id": 4,
                "user_raw_transcript": "sounds good",
                "classified_intent": "other",
                "tool_calls": [],
                "agent_response_text": _REPLY.agent_response_text,
                "schema_version": 1,
            }
        ]
    finally:
        await cleanup_test_call(call_id)


async def test_clinical_question_skips_decide_action(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    instrument = RecordingInstrument()
    await _start(call_id)
    try:
        graph = build_turn_graph(
            ScriptedClassifier(Intent.CLINICAL_QUESTION),
            instrument=instrument,
            planner=ScriptedPlanner(_NO_TOOLS),
            composer=ScriptedComposer(_REPLY),
        )
        nodes: list[str] = []
        async for update in graph.astream(
            TurnState(call_id=call_id, turn_id=1, user_transcript="can I take this with ibuprofen?"),
            stream_mode="updates",
        ):
            nodes.extend(update.keys())
        assert nodes == ["load_call_context", "intent_classification", "escalate", "commit_turn"]
        result = await run_turn(
            graph,
            TurnState(call_id=call_id, turn_id=2, user_transcript="can I take this with ibuprofen?"),
        )
        assert result.turn_outcome is TurnOutcome.ESCALATED
        assert result.classified_intent is Intent.CLINICAL_QUESTION
        assert result.agent_response_text == ESCALATION_RESPONSE
        assert result.call_facts is not None
        assert result.call_facts.call_outcome is CallOutcome.ESCALATED_APPROPRIATE
        stored = await get_call_context(call_id)
        assert stored.facts.current_turn_index == 2
        assert [turn.turn_id for turn in stored.recent_turns] == [1, 2]
        assert [event["turn_id"] for event in instrument.events] == [1, 2]
        assert all(event["classified_intent"] == "clinical_question" for event in instrument.events)
    finally:
        await cleanup_test_call(call_id)


async def test_decide_action_records_chosen_tools(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    await _start(call_id)
    try:
        graph = build_turn_graph(
            ScriptedClassifier(Intent.ASK_DISCOUNT),
            instrument=RecordingInstrument(),
            planner=ScriptedPlanner(
                ActionDecision(
                    tool_calls=[ToolCallChoice(tool=ToolName.APPLY_DISCOUNT, args={"offer_code": "SAVE10"})],
                )
            ),
            composer=ScriptedComposer(
                SpokenResponse(
                    agent_response_text="I can apply SAVE10.",
                    current_turn_facts={"applied_offer": "SAVE10"},
                )
            ),
        )
        nodes: list[str] = []
        async for update in graph.astream(
            TurnState(call_id=call_id, turn_id=1, user_transcript="any discount?"),
            stream_mode="updates",
        ):
            nodes.extend(update.keys())
        assert nodes == [
            "load_call_context",
            "intent_classification",
            "decide_action",
            "final_response",
            "commit_turn",
        ]
        result = await run_turn(
            graph,
            TurnState(call_id=call_id, turn_id=2, user_transcript="any discount?"),
        )
        assert result.classified_intent is Intent.ASK_DISCOUNT
        assert result.turn_outcome is TurnOutcome.CONTINUING
        assert len(result.tool_calls_this_turn) == 1
        assert result.tool_calls_this_turn[0].tool == ToolName.APPLY_DISCOUNT.value
        assert result.tool_calls_this_turn[0].args == {"offer_code": "SAVE10"}
        assert result.agent_response_text == "I can apply SAVE10."
        assert result.call_facts is not None
        assert result.call_facts.call_outcome is CallOutcome.IN_PROGRESS
        assert result.current_turn_facts == {"applied_offer": "SAVE10"}
        stored = await get_call_context(call_id)
        assert stored.facts.turn_facts == [
            {"applied_offer": "SAVE10"},
            {"applied_offer": "SAVE10"},
        ]
    finally:
        await cleanup_test_call(call_id)
