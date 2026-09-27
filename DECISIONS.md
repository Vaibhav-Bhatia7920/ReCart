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

## Deepgram Aura-2 streaming TTS

`voice.adapters.deepgram_tts.DeepgramTTS` implements `TTSProvider.synthesize_audio`. It uses `wss://api.deepgram.com/v1/speak` (not Flux `/v2/speak`). Auth is the same `Authorization: Token <DEEPGRAM_API_KEY>` header as ASR.

Query params: `model=aura-2-asteria-en`, `encoding=linear16`, `sample_rate=24000`. The speak schema's default model is `aura-asteria-en` (Aura 1); Aura-2 is the current generation in that same schema, and `aura-2-asteria-en` is the Aura-2 voice of the default name. Output is raw 16-bit PCM at 24 kHz. The speak query schema has no `channels` field; Deepgram's playback examples treat this encoding as mono. This adapter does not resample. `call_id` and `turn_id` are accepted to match the Protocol and are not sent — the speak messages have no call fields.

Text chunks are JSON text frames `{"type":"Speak","text":...}`. That `type` is `Speak` in the AsyncAPI spec and the generated Python SDK. One current Node sample on the websocket guide says `type: "Text"`; that does not match the spec, so this adapter does not send `Text`. When the text iterator ends, the client sends `{"type":"Close"}`, which the spec describes as flush-then-close after remaining audio. Binary frames are yielded as `bytes`. `Metadata`, `Flushed`, `Cleared`, and `Warning` (`description`, `code`) are not audio. `Warning` does not fail the stream.

Connect timeout is 10 seconds (`TTSTimeoutError`). A drop before `Close` retries once after 0.5 seconds, then raises `TTSConnectionDropped`. Text already accepted by a dead socket is not replayed. A normal close after `Close` ends the generator.

## Sweeper is off in pytest

`CART_ABANDONMENT_SWEEPER_ENABLED=false` in tests so the background task cannot race assertions. Tests call `sweep_abandoned_carts` directly (and the force-abandon endpoint) against a long timeout.

# Decisions (agent call state)

## Same database, two new tables

`call_facts` and `turn_logs` (Alembic `0003`) extend the existing Postgres database. There is no second database. One call is one `call_facts` row. Each graph invocation reads that row plus the last two `turn_logs`, then inserts one `turn_logs` row and updates `call_facts` in the same transaction.

## Telemetry is not copied onto turn_logs

`turn_logs` has no latency, audio, or event columns. Telemetry stays on the telemetry path, which already keys events by `call_id` and `turn_id` (`voice.template` / `telemetry.helper_functions`). That pair is the join. The telemetry writer is still an in-process queue and has no table of its own yet; this revision does not invent one and does not duplicate those fields onto `turn_logs`, so each fact keeps a single writer.

## `turn_logs.call_id` is ON DELETE RESTRICT

The foreign key uses `ON DELETE RESTRICT`. Postgres refuses to delete a `call_facts` row while its turns exist, so the turn log stays unless it is removed first. `cleanup_test_call` deletes `turn_logs` for that `call_id` first, then the `call_facts` row.

## One outcome column

`call_facts.call_outcome` defaults to `in_progress` and is the only escalation column. Once the value is a terminal outcome, `commit_turn` rejects an update that would replace it with a different outcome.

## Optimistic `current_turn_index`

`commit_turn` writes `current_turn_index = expected_turn_index + 1` only in `UPDATE ... WHERE call_id = ? AND current_turn_index = expected_turn_index`. If that matches zero rows, or the `(call_id, turn_id)` insert collides, the transaction rolls back and raises `StaleTurnError`.

# Decisions (agent graph)

## TurnState is a Pydantic model

`agent.state.TurnState` is a Pydantic model. LangGraph nodes return partial updates, and `run_turn` validates the graph output back into `TurnState`. `recent_turns` is capped at 2. `load_call_context` replaces `tool_calls_this_turn` with `[]` on every invocation.

## clinical_question escalates on a graph edge

`agent.intents.Intent` is the classifier's closed set. `OpenAIIntentClassifier` passes `IntentClassification` as the response schema, so the SDK parses the label into that model. `ALWAYS_ESCALATE` is `{clinical_question}`. `route_after_intent` sends that intent to `escalate` and every other known intent to `decide_action`.

## websockets pin moved to 16.1.1

`langgraph` 1.2.12 depends on `langgraph-sdk`, which requires `websockets<17`. The direct pin is now `websockets==16.1.1`. The Deepgram adapters call `connect(..., additional_headers=...)`, which 16.1.1 still accepts.
