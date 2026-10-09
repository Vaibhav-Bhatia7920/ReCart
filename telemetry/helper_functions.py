import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import os
import time
import json
from sqlalchemy import func
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import get_session_factory
from telemetry.models import CallEvents
from voice.template import ASR_Bytes

Event = str | dict[str, Any]

_write_queue: asyncio.Queue | None = None
_write_loop: asyncio.AbstractEventLoop | None = None


async def context_manager(input_audio: AsyncIterator[ASR_Bytes]) -> AsyncIterator[ASR_Bytes]:
    """
    Context manager is a function that takes a list of ASR bytes and returns an async iterator of ASR bytes.
    """
    async for audio in input_audio:
        yield audio


async def mark_event_in_db(call_id: str, event: Event, turn_id: str = "") -> None:
    """Append `event` onto the `call_events` row keyed by `call_id`."""
    await write_to_database(call_id, event, turn_id)


def _get_write_queue() -> asyncio.Queue:
    global _write_queue, _write_loop
    loop = asyncio.get_running_loop()
    if _write_queue is None or _write_loop is not loop:
        _write_queue = asyncio.Queue()
        _write_loop = loop
        asyncio.create_task(db_worker(_write_queue))
    return _write_queue


async def db_worker(queue: asyncio.Queue) -> None:
    while True:
        future = None
        try:
            call_id, event, turn_id, future = await queue.get()
        except asyncio.CancelledError:
            break
        try:
            await persist_call_event(call_id, event, turn_id)
            future.set_result(True)
        except Exception as e:
            if future is not None and not future.done():
                future.set_exception(e)
        finally:
            queue.task_done()


async def persist_call_event(call_id: str, event: Event, turn_id: str) -> None:
    payload = {
        "event": event,
        "turn_id": turn_id,
        "marked_at": datetime.now(UTC).isoformat(),
    }
    stmt = pg_insert(CallEvents).values(call_id=call_id, events=[payload])
    stmt = stmt.on_conflict_do_update(
        index_elements=[CallEvents.call_id],
        set_={
            "events": CallEvents.events.concat(stmt.excluded.events),
            "updated_at": func.now(),
        },
    )
    factory = get_session_factory()
    async with factory() as session:
        async with session.begin():
            await session.execute(stmt)


async def write_to_database(call_id: str, event: Event, turn_id: str) -> None:
    """Enqueue a mark and wait until the worker has written it."""
    queue = _get_write_queue()
    future = asyncio.get_running_loop().create_future()
    await queue.put((call_id, event, turn_id, future))
    await future

def log_event(call_id: str, event: str, turn_id: str) -> None:
    """Log an event to the database."""
    os.makedirs("logs", exist_ok=True)
    timestamp = time.perf_counter()
    with open(f"logs/{call_id}.json", "a") as f:
        f.write(json.dumps({"event": event, "turn_id": turn_id, "timestamp": timestamp}) + "\n")

async def fetch_call_events(call_id: str, turn_id: str | None = None) -> list[dict[str, Any]]:
    """Return marked events for a call, optionally filtered by turn_id."""
    factory = get_session_factory()
    async with factory() as session:
        row = await session.get(CallEvents, call_id)
    if row is None:
        return []
    events = list(row.events)
    if turn_id is None:
        return events
    wanted = str(turn_id)
    return [item for item in events if str(item.get("turn_id")) == wanted]
