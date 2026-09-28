from typing import Any, Protocol

from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field

from agent.errors import FinalResponseError
from agent.intents import Intent
from agent.state import TurnState
from app.settings import get_settings

_SYSTEM = (
    "Write one short spoken reply for the caller from call_facts and this turn's "
    "tool results. Do not invent tools that were not recorded. "
    "Light product guidance is allowed for vitamins, supplements, and typical OTC "
    "items using cart descriptions, use cases, and warnings. "
    "Do not give clinical advice for prescription or restricted medicines; "
    "those turns never reach this node. "
    "Also fill current_turn_facts with facts learned this turn only "
    "(address details, offer codes, payment-link status, why the cart was left, "
    "product the caller asked about). Use an empty object if nothing new was learned. "
    "Do not copy the full call_facts blob. "
    "The response schema is the only output."
)


class SpokenResponse(BaseModel):
    """Schema forced on the final_response model call."""

    model_config = ConfigDict(extra="forbid")

    agent_response_text: str
    current_turn_facts: dict[str, Any] = Field(default_factory=dict)


class ResponseComposer(Protocol):
    async def compose(self, state: TurnState) -> SpokenResponse: ...


class OpenAIResponseComposer:
    """Calls the model with SpokenResponse as the response schema."""

    def __init__(self, client: AsyncOpenAI, model: str) -> None:
        self._client = client
        self._model = model

    @classmethod
    def from_settings(cls) -> "OpenAIResponseComposer":
        settings = get_settings()
        if not settings.openai_api_key:
            raise FinalResponseError("OPENAI_API_KEY is not set")
        return cls(AsyncOpenAI(api_key=settings.openai_api_key), settings.intent_model)

    async def compose(self, state: TurnState) -> SpokenResponse:
        completion = await self._client.chat.completions.parse(
            model=self._model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": _user_message(state)},
            ],
            response_format=SpokenResponse,
        )
        parsed = completion.choices[0].message.parsed
        if not isinstance(parsed, SpokenResponse):
            raise FinalResponseError("model returned no structured reply")
        return parsed


def _user_message(state: TurnState) -> str:
    intent = state.classified_intent.value if isinstance(state.classified_intent, Intent) else "unset"
    facts = state.call_facts
    if facts is None:
        facts_text = "(none)"
    else:
        cart_lines = []
        for item in facts.cart_snapshot:
            cart_lines.append(
                f"{item.name}: {item.description}; "
                f"use_cases={', '.join(item.use_cases) or '(none)'}; "
                f"warnings={', '.join(item.warnings) or '(none)'}"
            )
        facts_text = (
            f"segment={facts.user_segment}; "
            f"applied_offer_id={facts.applied_offer_id}; "
            f"payment_link_sent={facts.payment_link_sent}; "
            f"confirmed_address={facts.confirmed_address is not None}; "
            f"cart={'; '.join(cart_lines) if cart_lines else '(empty)'}"
        )
    if state.tool_calls_this_turn:
        tools = []
        for record in state.tool_calls_this_turn:
            tools.append(
                f"{record.tool} args={record.args} result={record.result} error={record.error}"
            )
        tools_text = "\n".join(tools)
    else:
        tools_text = "(none)"
    return (
        f"classified_intent: {intent}\n"
        f"Caller said: {state.user_transcript}\n"
        f"call_facts: {facts_text}\n"
        f"Tool results this turn:\n{tools_text}"
    )
