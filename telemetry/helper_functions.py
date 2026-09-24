import asyncio
from typing import AsyncIterator

from voice.template import ASR_Bytes

_write_queue: asyncio.Queue | None = None


async def context_manager(input_audio: AsyncIterator[ASR_Bytes]) -> AsyncIterator[ASR_Bytes]:
    """
    Context manager is a function that takes a list of ASR bytes and returns an async iterator of ASR bytes.
    """
    async for audio in input_audio:
        yield audio


async def mark_event_in_db(audio: ASR_Bytes, event: str) -> None:
    """
    Mark event in db is a function that takes an ASR bytes and an event and marks the event in the database.
    """
    await write_to_database(audio, event)


def _get_write_queue() -> asyncio.Queue:
    global _write_queue
    if _write_queue is None:
        _write_queue = asyncio.Queue()
        asyncio.create_task(db_worker(_write_queue))
    return _write_queue


async def db_worker(queue: asyncio.Queue) -> None:
    while True:
        future = None
        try:
            audio, event, future = await queue.get()
            await asyncio.sleep(0.5)  ## currently mimicking a db call
            result = True
            future.set_result(result)
        except Exception as e:
            if future is not None and not future.done():
                future.set_exception(e)
        finally:
            queue.task_done()


async def write_to_database(audio: ASR_Bytes, event: str) -> None:
    """
    Write to database is a function that takes data and writes it to the database.
    """
    queue = _get_write_queue()
    future = asyncio.get_running_loop().create_future()
    await queue.put((audio, event, future))
    await future


# def periodic_db_write(queue : AsyncQueue[]) -> None:
#     """
#     Periodic db write is a function that takes a queue and writes the data to the database periodically.
#     """
#     while True:
#         data = queue.get()
#         write_to_database(data)
#         time.sleep(1)
