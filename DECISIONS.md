# Decisions (phase 0)

## Tooling: pip-tools, not uv

Chose **pip-tools**. This repo is specified to install into a local venv from a `requirements.txt`, and pip-tools compiles `pyproject.toml` groups into that file. uv was not on PATH and would have added a second installer next to the requested venv + pip workflow.

Pinned groups live in `pyproject.toml` (`project.dependencies` = runtime, `optional-dependencies.dev` = dev). `requirements.txt` is the compiled lock used by the venv, Docker image, and CI.

To refresh pins after changing `pyproject.toml`:

```bash
.venv/bin/pip-compile pyproject.toml --extra dev -o requirements.txt
```

## Local interpreter vs project target

`python3` on this machine is `/usr/bin/python3` (CPython **3.9.6**). The venv was created with that interpreter because it is what `which python3` resolved to. The project target remains **3.12** (`requires-python`, Docker `python:3.12-slim`, CI `setup-python` 3.12). Install the local venv on 3.12 when it is available if you want local and image runtimes to match.

## Thin `app/` package

`store/`, `orchestrator/`, `agent/`, `voice/`, `telemetry/`, and `handoff/` stay empty and importable. Settings, FastAPI, `/health`, and async DB/Redis clients live in `app/` so domain packages stay unused in phase 0.

## SQLAlchemy + asyncpg (not raw asyncpg only)

Alembic is required and is built around SQLAlchemy metadata. Using SQLAlchemy's asyncio engine plus asyncpg keeps one URL (`postgresql+asyncpg://...`) for the health check and for migrations. No ORM models in this phase.

## Alembic env is async; initial revision is empty

`alembic/env.py` runs migrations through an async engine so the process stays async end-to-end. Revision `0001` has empty `upgrade`/`downgrade` because there is no schema yet.

## Single `requirements.txt` for venv and image

The request was to install the venv only via `requirements.txt`, so runtime and dev pins share that file. The Docker image therefore also has pytest/ruff/mypy. Acceptable for a sandbox skeleton; split runtime vs `requirements-dev.txt` later if the image needs to stay slim.

## Compose overrides connection hosts

`.env.example` uses `localhost` so host-side pytest can reach published ports. The `app` service in Compose sets `DATABASE_URL` / `REDIS_URL` to the `postgres` and `redis` hostnames. Postgres data uses the named volume `postgres_data`.

## `/health` is a real connectivity check

The handler pings Postgres (`SELECT 1`) and Redis (`PING`) and returns 503 if either fails. The one pytest hits the ASGI app with httpx; it needs Postgres and Redis (Compose locally, GitHub Actions services in CI).

# Decisions (phase 1)

## Store APIs are intentionally non-idempotent

Action endpoints (`add_item`, `apply_offer`, `create_payment_link`, and the rest) do **not** take idempotency keys. Retrying `add_item` or `create_payment_link` will mutate again (more quantity, another pending order). Dedup belongs on the agent's tool layer in a later phase.

## Stock is reserved on add

This is the intended inventory strategy, not a checkout-time decrement. `add_item` reduces `products.stock` immediately and snapshots `cart_items.unit_price`. Abandonment (sweeper or force) restores reserved units and cancels `pending_payment` orders. `mark_order_paid` converts the cart and leaves stock consumed so two active carts cannot oversell.

## Offer validity is computed, not stored as a flag

Offers have a kind plus threshold/percent. Validity is `subtotal >= min_subtotal` (and a positive percent for percent-off). Totals recompute on every read; an applied offer that no longer meets the threshold is treated as inactive for pricing and blocks `create_payment_link` until the cart qualifies again or another offer is applied.

## Duplicate-safe abandonment events

The sweeper (and the test-only force-abandon path) share `publish_abandonment_once`: Redis `SET NX` on `store:abandonment:{cart_id}`, then one `XADD` to `store:cart-abandoned`, then `carts.abandonment_emitted_at`. Restarts can finish an abandoned cart that never stamped `emitted_at` without a second stream message. If `XADD` fails, the Redis key is deleted so the next pass can retry. Prefer no duplicates over guaranteed delivery.

## Test-only payment and abandon routes stay unauthenticated

This is a sandbox. `POST /test/orders/{id}/mark-paid`, `POST /test/carts/{id}/abandon`, and `GET /test/carts/{id}/snapshot` are always mounted. No auth, users, or real PSP.

## Deepgram Nova-3 streaming ASR

`voice.adapters.deepgram_asr.DeepgramASR` talks to `wss://api.deepgram.com/v1/listen` with query `model=nova-3` (general Nova-3, not `nova-3-medical`). Auth is the `Authorization: Token <DEEPGRAM_API_KEY>` header.

Raw audio is forwarded unchanged as binary WebSocket frames. The socket is opened with `encoding=linear16`, `sample_rate=16000`, `channels=1` — the raw PCM settings in Deepgram's Nova-3 streaming examples (16-bit signed little-endian PCM, mono, 16 kHz). Deepgram does not auto-detect raw PCM; those three query params must match the bytes. This adapter does not resample. If the source is something else (for example 8 kHz μ-law telephony), convert to 16 kHz mono linear16 before `transcribe_audio`, or the stream will be mis-decoded.

Interim vs final follows Deepgram `Results.is_final`, not `speech_final`. `is_final: false` (requires `interim_results=true`) is a `PartialTranscript`. `is_final: true` is one `FullTranscript` for that audio segment. Deepgram's own docs say a spoken utterance can contain several `is_final: true` segments before `speech_final: true`; `speech_final` is the pause/utterance boundary and is not what this adapter treats as final. `channel.alternatives[0].transcript` is the text. Partial `stability` and final `confidence` both come from `channel.alternatives[0].confidence`. Partial `timestamp` is Results `start`; final `end_time` is `start + duration` (seconds on Deepgram's audio clock).

Connect timeout is 10 seconds (`ASRTimeoutError`). A WebSocket drop before `{"type":"CloseStream"}` retries once after 0.5 seconds, then raises `ASRConnectionDropped`. A normal close after `CloseStream` ends the generator without that error. Already-sent audio is not replayed on reconnect.

## Sweeper is off in pytest

`CART_ABANDONMENT_SWEEPER_ENABLED=false` in tests so the background task cannot race assertions. Tests call `sweep_abandoned_carts` directly (and the force-abandon endpoint) against a long timeout.
