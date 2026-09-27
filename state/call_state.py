from typing import Any

from sqlalchemy import func, select, update
from sqlalchemy.exc import IntegrityError

from app.db import get_session_factory
from state.errors import (
    CallAlreadyExistsError,
    CallNotFoundError,
    StaleTurnError,
    TerminalOutcomeError,
)
from state.models import TERMINAL_OUTCOMES, CallFacts, CallOutcome, TurnLog
from state.schemas import (
    CallContext,
    CallFactsUpdate,
    CallFactsView,
    CartMedicine,
    TurnLogInput,
    TurnLogView,
)


def _is_unique_violation(exc: IntegrityError) -> bool:
    orig = exc.orig
    sqlstate = getattr(orig, "sqlstate", None) or getattr(orig, "pgcode", None)
    return sqlstate == "23505"


def _normalize_updates(call_facts_updates: dict[str, Any]) -> dict[str, Any]:
    parsed = CallFactsUpdate.model_validate(call_facts_updates)
    values = parsed.set_fields()
    if "cart_snapshot" in values and values["cart_snapshot"] is not None:
        values["cart_snapshot"] = [
            CartMedicine.model_validate(item).model_dump() for item in values["cart_snapshot"]
        ]
    if "call_outcome" in values and values["call_outcome"] is not None:
        values["call_outcome"] = CallOutcome(values["call_outcome"])
    return values


async def get_call_context(call_id: str) -> CallContext:
    """Read call_facts and the last two turn_logs rows (chronological) for this call."""
    factory = get_session_factory()
    async with factory() as session:
        facts = await session.get(CallFacts, call_id)
        if facts is None:
            raise CallNotFoundError(call_id)
        turn_rows = list(
            (
                await session.scalars(
                    select(TurnLog)
                    .where(TurnLog.call_id == call_id)
                    .order_by(TurnLog.turn_id.desc())
                    .limit(2)
                )
            ).all()
        )
        turn_rows.reverse()
        return CallContext(
            facts=CallFactsView.model_validate(facts),
            recent_turns=[TurnLogView.model_validate(row) for row in turn_rows],
        )


async def start_call(
    call_id: str,
    customer_id: str,
    phone_number: str,
    user_segment: str,
    initial_cart_snapshot: list[dict[str, Any]] | list[CartMedicine],
) -> CallFactsView:
    """Insert call_facts with current_turn_index=0 and call_outcome=in_progress."""
    medicines = [CartMedicine.model_validate(item) for item in initial_cart_snapshot]
    if not medicines:
        raise ValueError("initial_cart_snapshot must include at least one medicine")
    row = CallFacts(
        call_id=call_id,
        customer_id=customer_id,
        phone_number=phone_number,
        user_segment=user_segment,
        cart_snapshot=[item.model_dump() for item in medicines],
        schema_version=1,
        confirmed_address=None,
        applied_offer_id=None,
        payment_link_sent=False,
        call_outcome=CallOutcome.IN_PROGRESS,
        current_turn_index=0,
    )
    factory = get_session_factory()
    async with factory() as session:
        try:
            async with session.begin():
                session.add(row)
                await session.flush()
                await session.refresh(row)
                return CallFactsView.model_validate(row)
        except IntegrityError as exc:
            raise CallAlreadyExistsError(call_id) from exc


async def commit_turn(
    call_id: str,
    expected_turn_index: int,
    turn_log: TurnLogInput,
    call_facts_updates: dict[str, Any],
) -> CallFactsView:
    """Insert one turn_logs row and advance call_facts in a single transaction.

    The facts update matches only when current_turn_index is still
    expected_turn_index. Zero rows updated, or a repeated (call_id, turn_id),
    raises StaleTurnError and rolls back the insert. A terminal call_outcome
    cannot be replaced by a different outcome.
    """
    updates = _normalize_updates(call_facts_updates)
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            facts = await session.get(CallFacts, call_id, with_for_update=True)
            if facts is None:
                raise CallNotFoundError(call_id)
            new_outcome = updates.get("call_outcome")
            if (
                isinstance(new_outcome, CallOutcome)
                and facts.call_outcome in TERMINAL_OUTCOMES
                and new_outcome != facts.call_outcome
            ):
                raise TerminalOutcomeError(call_id, facts.call_outcome, new_outcome)
            if facts.current_turn_index != expected_turn_index:
                raise StaleTurnError(call_id, expected_turn_index)

            session.add(
                TurnLog(
                    call_id=call_id,
                    turn_id=turn_log.turn_id,
                    schema_version=turn_log.schema_version,
                    user_raw_transcript=turn_log.user_raw_transcript,
                    classified_intent=turn_log.classified_intent,
                    tool_calls=[item.model_dump() for item in turn_log.tool_calls],
                    agent_response_text=turn_log.agent_response_text,
                )
            )
            try:
                await session.flush()
            except IntegrityError as exc:
                if _is_unique_violation(exc):
                    raise StaleTurnError(call_id, expected_turn_index) from exc
                raise

            result = await session.execute(
                update(CallFacts)
                .where(
                    CallFacts.call_id == call_id,
                    CallFacts.current_turn_index == expected_turn_index,
                )
                .values(
                    current_turn_index=expected_turn_index + 1,
                    updated_at=func.now(),
                    **updates,
                )
                .returning(CallFacts.call_id)
            )
            if result.scalar_one_or_none() is None:
                raise StaleTurnError(call_id, expected_turn_index)
            await session.refresh(facts)
            return CallFactsView.model_validate(facts)


async def cleanup_test_call(call_id: str) -> None:
    """Delete turn_logs first, then call_facts, so ON DELETE RESTRICT does not block the test row."""
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            facts = await session.get(CallFacts, call_id)
            if facts is None:
                raise CallNotFoundError(call_id)
            turns = (
                await session.scalars(select(TurnLog).where(TurnLog.call_id == call_id))
            ).all()
            for turn in turns:
                await session.delete(turn)
            await session.flush()
            await session.delete(facts)
