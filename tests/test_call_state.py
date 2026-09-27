import asyncio
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import delete
from sqlalchemy.exc import IntegrityError

from app.db import get_session_factory
from state.call_state import cleanup_test_call, commit_turn, get_call_context, start_call
from state.errors import StaleTurnError, TerminalOutcomeError
from state.models import CallFacts, CallOutcome
from state.schemas import TurnLogInput

MEDICINE = {
    "name": "Amoxicillin 500mg",
    "description": "Penicillin antibiotic used for bacterial infections.",
    "use_cases": ["sinus infection", "strep throat"],
    "warnings": ["Do not use if allergic to penicillin"],
    "sku": "AMOX-500",
}


def _turn(turn_id: int, transcript: str) -> TurnLogInput:
    return TurnLogInput(
        turn_id=turn_id,
        user_raw_transcript=transcript,
        classified_intent="confirm_cart",
        tool_calls=[],
        agent_response_text=f"reply-{turn_id}",
    )


async def _start(call_id: str) -> None:
    await start_call(
        call_id,
        customer_id="cust-1",
        phone_number="+15551212",
        user_segment="frequent_buyer",
        initial_cart_snapshot=[MEDICINE],
    )


async def test_concurrent_commit_turn_one_wins(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    await _start(call_id)
    try:
        results = await asyncio.gather(
            commit_turn(call_id, 0, _turn(1, "first writer"), {}),
            commit_turn(call_id, 0, _turn(2, "second writer"), {}),
            return_exceptions=True,
        )
        successes = [item for item in results if not isinstance(item, Exception)]
        stale = [item for item in results if isinstance(item, StaleTurnError)]
        assert len(successes) == 1
        assert len(stale) == 1
        assert successes[0].current_turn_index == 1
        context = await get_call_context(call_id)
        assert context.facts.current_turn_index == 1
        assert len(context.recent_turns) == 1
    finally:
        await cleanup_test_call(call_id)


async def test_get_call_context_returns_last_two_turns(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    await _start(call_id)
    try:
        for index in range(3):
            await commit_turn(call_id, index, _turn(index + 1, f"utterance-{index + 1}"), {})
        context = await get_call_context(call_id)
        assert [turn.turn_id for turn in context.recent_turns] == [2, 3]
        assert [turn.user_raw_transcript for turn in context.recent_turns] == [
            "utterance-2",
            "utterance-3",
        ]
    finally:
        await cleanup_test_call(call_id)


async def test_call_outcome_starts_in_progress_and_one_terminal(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    started = await start_call(
        call_id,
        customer_id="cust-9",
        phone_number="+15550000",
        user_segment="churn_risk",
        initial_cart_snapshot=[MEDICINE],
    )
    try:
        assert started.call_outcome is CallOutcome.IN_PROGRESS
        assert "escalated" not in CallFacts.__table__.columns
        resolved = await commit_turn(
            call_id,
            0,
            _turn(1, "that works"),
            {"call_outcome": CallOutcome.RESOLVED_CORRECT},
        )
        assert resolved.call_outcome is CallOutcome.RESOLVED_CORRECT
        with pytest.raises(TerminalOutcomeError):
            await commit_turn(
                call_id,
                1,
                _turn(2, "change the label"),
                {"call_outcome": CallOutcome.DROPPED},
            )
        context = await get_call_context(call_id)
        assert context.facts.call_outcome is CallOutcome.RESOLVED_CORRECT
        assert context.facts.current_turn_index == 1
        assert [turn.turn_id for turn in context.recent_turns] == [1]
    finally:
        await cleanup_test_call(call_id)


async def test_delete_call_facts_restricted_while_turns_exist(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    await _start(call_id)
    await commit_turn(call_id, 0, _turn(1, "still here"), {})
    factory = get_session_factory()
    with pytest.raises(IntegrityError):
        async with factory() as session:
            async with session.begin():
                await session.execute(delete(CallFacts).where(CallFacts.call_id == call_id))
    context = await get_call_context(call_id)
    assert context.facts.call_id == call_id
    assert len(context.recent_turns) == 1
    await cleanup_test_call(call_id)
    factory = get_session_factory()
    async with factory() as check:
        assert await check.get(CallFacts, call_id) is None
