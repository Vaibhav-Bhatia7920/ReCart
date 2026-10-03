import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text

from app.db import close_db, get_engine, get_redis, init_engine, init_redis
from app.settings import get_settings
from app.voice_ws import default_asr_factory, default_tts_factory, router as voice_ws_router
from store.router import router as store_router
from store.sweeper import run_abandonment_sweeper


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()
    app.state.engine = init_engine(settings.database_url)
    app.state.redis = init_redis(settings.redis_url)
    app.state.asr_factory = default_asr_factory
    app.state.tts_factory = default_tts_factory
    stop = asyncio.Event()
    sweeper_task: asyncio.Task[None] | None = None
    if settings.cart_abandonment_sweeper_enabled:
        sweeper_task = asyncio.create_task(run_abandonment_sweeper(stop))
    try:
        yield
    finally:
        stop.set()
        if sweeper_task is not None:
            await sweeper_task
        await close_db()


app = FastAPI(title="cart-recovery", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=get_settings().cors_origin_list(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
app.include_router(store_router)
app.include_router(voice_ws_router)


@app.get("/health")
async def health() -> JSONResponse:
    database_ok = await _check_database()
    redis_ok = await _check_redis()
    payload = {
        "status": "ok" if database_ok and redis_ok else "unhealthy",
        "database": "ok" if database_ok else "error",
        "redis": "ok" if redis_ok else "error",
    }
    status_code = 200 if payload["status"] == "ok" else 503
    return JSONResponse(content=payload, status_code=status_code)


async def _check_database() -> bool:
    try:
        async with get_engine().connect() as connection:
            await connection.execute(text("SELECT 1"))
        return True
    except Exception:
        return False


async def _check_redis() -> bool:
    try:
        return bool(await get_redis().ping())
    except Exception:
        return False
