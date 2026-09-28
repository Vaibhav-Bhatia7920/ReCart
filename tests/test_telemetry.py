from typing import Any
from uuid import uuid4

from app.db import get_session_factory
from telemetry.instrument import TelemetryInstrument
from telemetry.models import CallEvents


async def test_mark_event_appends_under_call_id(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    asr = TelemetryInstrument("deepgram_asr", "asr", call_id, "1")
    tts = TelemetryInstrument("deepgram_tts", "tts", call_id, "1")
    await asr.mark_event("First Partial Transcript")
    await asr.mark_event("Final Transcript")
    await tts.mark_event("First Audio Frame")
    await tts.mark_event("Flushed")

    factory = get_session_factory()
    async with factory() as session:
        row = await session.get(CallEvents, call_id)

    assert row is not None
    names = [item["event"] for item in row.events]
    assert names == [
        "First Partial Transcript",
        "Final Transcript",
        "First Audio Frame",
        "Flushed",
    ]
    assert all(item["turn_id"] == "1" for item in row.events)


async def test_mark_event_stores_turn_log(client: Any) -> None:
    del client
    call_id = f"call-{uuid4()}"
    turn_log = {
        "turn_id": 1,
        "user_raw_transcript": "any discount?",
        "classified_intent": "ask_discount",
        "tool_calls": [],
        "agent_response_text": "I can apply SAVE10.",
        "schema_version": 1,
    }
    await TelemetryInstrument("commit_turn", "agent turn log", call_id, "1").mark_event(turn_log)

    factory = get_session_factory()
    async with factory() as session:
        row = await session.get(CallEvents, call_id)

    assert row is not None
    assert row.events[0]["event"] == turn_log
