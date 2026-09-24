import asyncio
import logging

from app.db import get_redis, get_session_factory
from app.settings import get_settings
from store.services import sweep_abandoned_carts

logger = logging.getLogger(__name__)


async def run_abandonment_sweeper(stop: asyncio.Event) -> None:
    settings = get_settings()
    interval = settings.cart_abandonment_sweep_interval_seconds
    while not stop.is_set():
        try:
            factory = get_session_factory()
            async with factory() as session:
                await sweep_abandoned_carts(session, get_redis())
                await session.commit()
        except Exception:
            logger.exception("Abandonment sweeper iteration failed")
        try:
            await asyncio.wait_for(stop.wait(), timeout=interval)
        except TimeoutError:
            continue
