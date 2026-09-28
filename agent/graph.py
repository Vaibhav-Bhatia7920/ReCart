from typing import Any, Literal, cast

from langgraph.graph import END, START, StateGraph
from langgraph.graph.state import CompiledStateGraph

from agent.classify import IntentClassifier, OpenAIIntentClassifier
from agent.errors import UnclassifiedIntentError
from agent.intents import ALWAYS_ESCALATE
from agent.respond import OpenAIResponseComposer, ResponseComposer
from agent.state import RECENT_TURN_LIMIT, TurnOutcome, TurnState
from agent.timer import TurnTimer
from agent.tools import ActionPlanner, OpenAIActionPlanner, records_from_decision
from state.call_state import commit_turn as persist_turn
from state.call_state import get_call_context
from state.models import CallOutcome
from state.schemas import TurnLogInput
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
    """Terminal handoff. Runtime always logs escalated_appropriate."""
    if state.call_facts is None:
        raise RuntimeError("load_call_context must run before escalate")
    return {
        "turn_outcome": TurnOutcome.ESCALATED,
        "agent_response_text": ESCALATION_RESPONSE,
        "call_facts": state.call_facts.model_copy(
            update={"call_outcome": CallOutcome.ESCALATED_APPROPRIATE}
        ),
        "current_turn_facts": {},
    }


async def decide_action(state: TurnState, planner: ActionPlanner) -> dict[str, Any]:
    if state.call_facts is None or state.classified_intent is None:
        raise RuntimeError("load_call_context and intent_classification must run before decide_action")
    decision = await planner.plan(state)
    return {
        "tool_calls_this_turn": records_from_decision(state.call_id, state.turn_id, decision),
    }


async def final_response(state: TurnState, composer: ResponseComposer) -> dict[str, Any]:
    if state.call_facts is None or state.classified_intent is None:
        raise RuntimeError("load_call_context and intent_classification must run before final_response")
    spoken = await composer.compose(state)
    return {
        "turn_outcome": TurnOutcome.CONTINUING,
        "agent_response_text": spoken.agent_response_text,
        "current_turn_facts": spoken.current_turn_facts,
    }


async def commit_turn(
    state: TurnState,
    instrument: TelemetryInstrument | None = None,
) -> dict[str, Any]:
    """Write this turn's log and append current_turn_facts onto call_facts.turn_facts."""
    if state.call_facts is None or state.classified_intent is None:
        raise RuntimeError("load_call_context and intent_classification must run before commit_turn")
    turn_log = TurnLogInput(
        turn_id=state.turn_id,
        user_raw_transcript=state.user_transcript,
        classified_intent=state.classified_intent.value,
        tool_calls=state.tool_calls_this_turn,
        agent_response_text=state.agent_response_text,
    )
    facts_list = list(state.call_facts.turn_facts)
    if state.current_turn_facts:
        facts_list.append(state.current_turn_facts)
    updates: dict[str, Any] = {"turn_facts": facts_list}
    if state.turn_outcome is TurnOutcome.ESCALATED:
        updates["call_outcome"] = CallOutcome.ESCALATED_APPROPRIATE
    updated = await persist_turn(
        state.call_id,
        state.call_facts.current_turn_index,
        turn_log,
        updates,
    )
    telemetry = instrument or TelemetryInstrument(
        name="commit_turn",
        description="agent turn log",
        call_id=state.call_id,
        turn_id=str(state.turn_id),
    )
    await telemetry.mark_event(turn_log.model_dump(mode="json"))
    return {"call_facts": updated}


TurnGraph = CompiledStateGraph[TurnState, None, TurnState, TurnState]


def build_turn_graph(
    classifier: IntentClassifier | None = None,
    instrument: TelemetryInstrument | None = None,
    planner: ActionPlanner | None = None,
    composer: ResponseComposer | None = None,
) -> TurnGraph:
    resolved = classifier if classifier is not None else OpenAIIntentClassifier.from_settings()
    resolved_planner = planner if planner is not None else OpenAIActionPlanner.from_settings()
    resolved_composer = composer if composer is not None else OpenAIResponseComposer.from_settings()

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

    async def decide_action_node(state: TurnState) -> dict[str, Any]:
        return await decide_action(state, resolved_planner)

    async def final_response_node(state: TurnState) -> dict[str, Any]:
        async with TurnTimer(
            "final_response",
            state.call_id,
            str(state.turn_id),
            instrument,
        ):
            return await final_response(state, resolved_composer)

    async def commit_turn_node(state: TurnState) -> dict[str, Any]:
        return await commit_turn(state, instrument)

    builder = StateGraph(TurnState)
    builder.add_node("load_call_context", load_call_context)
    builder.add_node("intent_classification", intent_classification)
    builder.add_node("escalate", escalate)
    builder.add_node("decide_action", decide_action_node)
    builder.add_node("final_response", final_response_node)
    builder.add_node("commit_turn", commit_turn_node)
    builder.add_edge(START, "load_call_context")
    builder.add_edge("load_call_context", "intent_classification")
    builder.add_conditional_edges(
        "intent_classification",
        route_after_intent,
        {"escalate": "escalate", "decide_action": "decide_action"},
    )
    builder.add_edge("escalate", "commit_turn")
    builder.add_edge("decide_action", "final_response")
    builder.add_edge("final_response", "commit_turn")
    builder.add_edge("commit_turn", END)
    return cast(TurnGraph, builder.compile())


async def run_turn(graph: TurnGraph, turn: TurnState) -> TurnState:
    raw = await graph.ainvoke(turn)
    return TurnState.model_validate(raw)
