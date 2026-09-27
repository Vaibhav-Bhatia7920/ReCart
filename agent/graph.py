from typing import Any, Literal, cast

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from agent.classify import IntentClassifier, OpenAIIntentClassifier
from agent.errors import UnclassifiedIntentError
from agent.intents import ALWAYS_ESCALATE
from agent.state import RECENT_TURN_LIMIT, TurnOutcome, TurnState
from agent.timer import TurnTimer
from state.call_state import get_call_context
from telemetry.instrument import TelemetryInstrument

ESCALATION_RESPONSE = "I'm going to connect you with a pharmacist for that question."


async def load_call_context(state: TurnState) -> dict[str, Any]:
    """Entry node. Loads call_facts and the last two turns, and clears this turn's tool calls."""
    context = await get_call_context(state.call_id)
    if len(context.recent_turns) > RECENT_TURN_LIMIT:
        raise RuntimeError(f"get_call_context returned more than {RECENT_TURN_LIMIT} turns")
    return {
        "call_facts": context.facts,
        "recent_turns": context.recent_turns,
        "tool_calls_this_turn": [],
    }


def route_after_intent(state: TurnState) -> Literal["escalate", "decide_action"]:
    """Hard edge. clinical_question never reaches decide_action."""
    intent = state.classified_intent
    if intent is None:
        raise UnclassifiedIntentError(f"Call {state.call_id} has no classified intent")
    if intent in ALWAYS_ESCALATE:
        return "escalate"
    return "decide_action"


async def escalate(state: TurnState) -> dict[str, Any]:
    del state
    return {
        "turn_outcome": TurnOutcome.ESCALATED,
        "agent_response_text": ESCALATION_RESPONSE,
    }


async def decide_action(state: TurnState) -> dict[str, Any]:
    """Non-escalation target. Action selection is not implemented in this step."""
    del state
    return {"turn_outcome": TurnOutcome.CONTINUING}


TurnGraph = CompiledStateGraph[TurnState, None, TurnState, TurnState]


def build_turn_graph(
    classifier: IntentClassifier | None = None,
    instrument: TelemetryInstrument | None = None,
) -> TurnGraph:
    resolved = classifier if classifier is not None else OpenAIIntentClassifier.from_settings()

    async def intent_classification(state: TurnState) -> dict[str, Any]:
        if state.call_facts is None:
            raise RuntimeError("load_call_context must run before intent_classification")
        async with TurnTimer(
            "intent_classification",
            state.call_id,
            str(state.turn_id),
            instrument,
        ):
            classified = await resolved.classify(state)
        return {"classified_intent": classified.intent}

    builder = StateGraph(TurnState)
    builder.add_node("load_call_context", load_call_context)
    builder.add_node("intent_classification", intent_classification)
    builder.add_node("escalate", escalate)
    builder.add_node("decide_action", decide_action)
    builder.add_edge(START, "load_call_context")
    builder.add_edge("load_call_context", "intent_classification")
    builder.add_conditional_edges(
        "intent_classification",
        route_after_intent,
        {"escalate": "escalate", "decide_action": "decide_action"},
    )
    builder.add_edge("escalate", END)
    builder.add_edge("decide_action", END)
    return cast(TurnGraph, builder.compile())


async def run_turn(graph: TurnGraph, turn: TurnState) -> TurnState:
    raw = await graph.ainvoke(turn)
    return TurnState.model_validate(raw)
