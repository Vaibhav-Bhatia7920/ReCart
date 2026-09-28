import os

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql+asyncpg://cart:cart@localhost:5432/cart_recovery",
)
os.environ.setdefault("REDIS_URL", "redis://localhost:6379/0")
os.environ["CART_ABANDONMENT_SWEEPER_ENABLED"] = "false"

from collections.abc import AsyncIterator  # noqa: E402
from decimal import Decimal  # noqa: E402
from typing import Any  # noqa: E402

import pytest  # noqa: E402
from alembic import command  # noqa: E402
from alembic.config import Config  # noqa: E402
from httpx import ASGITransport, AsyncClient  # noqa: E402
from sqlalchemy import text  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: E402

from app.db import get_engine, get_redis, get_session_factory  # noqa: E402
from app.main import app, lifespan  # noqa: E402
from app.settings import get_settings  # noqa: E402
from store.models import Offer, OfferKind, Product  # noqa: E402

get_settings.cache_clear()


@pytest.fixture(scope="session", autouse=True)
def apply_migrations() -> None:
    command.upgrade(Config("alembic.ini"), "head")


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    async with lifespan(app):
        await _reset_store()
        async with AsyncClient(
            transport=ASGITransport(app=app),
            base_url="http://test",
        ) as async_client:
            yield async_client


@pytest.fixture
async def session(client: AsyncClient) -> AsyncIterator[AsyncSession]:
    del client
    factory = get_session_factory()
    async with factory() as db_session:
        yield db_session
        await db_session.commit()


@pytest.fixture
async def catalog(session: AsyncSession) -> dict[str, Any]:
    cheap = Product(
        name="Cough Drops",
        category="Cold",
        price=Decimal("3.00"),
        stock=10,
        requires_prescription=False,
    )
    bundle = Product(
        name="Multivitamin Tablets",
        category="Vitamins",
        price=Decimal("20.00"),
        stock=8,
        requires_prescription=False,
    )
    session.add_all([cheap, bundle])
    session.add(
        Offer(
            code="SAVE10",
            kind=OfferKind.PERCENT_OFF_OVER_THRESHOLD,
            percent_off=Decimal("10.00"),
            min_subtotal=Decimal("25.00"),
        )
    )
    session.add(
        Offer(
            code="FREEDEL",
            kind=OfferKind.FREE_DELIVERY,
            percent_off=None,
            min_subtotal=Decimal("15.00"),
        )
    )
    await session.flush()
    await session.commit()
    return {"cheap": cheap, "bundle": bundle}


async def _reset_store() -> None:
    engine = get_engine()
    redis = get_redis()
    settings = get_settings()
    async with engine.begin() as connection:
        await connection.execute(
            text(
                "TRUNCATE turn_logs, call_events, call_facts, cart_items, orders, addresses, carts, "
                "products, offers RESTART IDENTITY CASCADE"
            )
        )
    await redis.delete(settings.redis_abandonment_stream)
    async for key in redis.scan_iter(f"{settings.redis_abandonment_emitted_prefix}:*"):
        await redis.delete(key)
