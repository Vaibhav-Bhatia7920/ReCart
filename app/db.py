from collections.abc import AsyncIterator

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None
_redis: Redis | None = None


def init_engine(database_url: str) -> AsyncEngine:
    global _engine, _session_factory
    _engine = create_async_engine(database_url)
    _session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    return _engine


def init_redis(redis_url: str) -> Redis:
    global _redis
    client: Redis = Redis.from_url(redis_url, decode_responses=True)
    _redis = client
    return client


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Database engine is not initialized")
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        raise RuntimeError("Session factory is not initialized")
    return _session_factory


def get_redis() -> Redis:
    if _redis is None:
        raise RuntimeError("Redis is not initialized")
    return _redis


async def close_db() -> None:
    global _engine, _session_factory, _redis
    if _redis is not None:
        await _redis.aclose()
        _redis = None
    if _engine is not None:
        await _engine.dispose()
        _engine = None
        _session_factory = None


async def session_scope() -> AsyncIterator[AsyncSession]:
    factory = get_session_factory()
    async with factory() as session:
        yield session
