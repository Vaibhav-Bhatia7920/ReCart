from enum import Enum
from typing import Any, Protocol

from openai import AsyncOpenAI
from pydantic import BaseModel, ConfigDict, Field

from agent.errors import ActionPlannerError
from agent.intents import Intent
from agent.state import TurnState
from app.settings import get_settings
from state.schemas import ToolCallRecord


class ToolName(str, Enum):
    APPLY_DISCOUNT = "apply_discount"
    GENERATE_FINAL_PAYMENT_LINK = "generate_final_payment_link"
    SEARCH_STORE = "search_store"


TOOLS: tuple[ToolName, ToolName, ToolName] = (
    ToolName.APPLY_DISCOUNT,
    ToolName.GENERATE_FINAL_PAYMENT_LINK,
    ToolName.SEARCH_STORE,
)

_SYSTEM = (
    "Choose zero or more tools for this turn. Only these tools exist: "
    f"{', '.join(tool.value for tool in TOOLS)}. "
    "apply_discount: caller wants a discount or offer; args may include offer_code. "
    "generate_final_payment_link: caller is ready to pay or has a payment issue. "
    "search_store: caller asks what the store carries. "
    "Use call_facts, recent_turns, classified_intent, and the latest transcript. "
    "Do not write the caller-facing reply. "
    "The response schema is the only output."
)


class ToolCallChoice(BaseModel):
    model_config = ConfigDict(extra="forbid")

    tool: ToolName
    args: dict[str, Any] = Field(default_factory=dict)


class ActionDecision(BaseModel):
    """Schema forced on the decide_action model call."""

    model_config = ConfigDict(extra="forbid")

    tool_calls: list[ToolCallChoice]


class ActionPlanner(Protocol):
    async def plan(self, state: TurnState) -> ActionDecision: ...


class OpenAIActionPlanner:
    """Calls the model with ActionDecision as the response schema."""

    def __init__(self, client: AsyncOpenAI, model: str) -> None:
        self._client = client
        self._model = model

    @classmethod
    def from_settings(cls) -> "OpenAIActionPlanner":
        settings = get_settings()
        if not settings.openai_api_key:
            raise ActionPlannerError("OPENAI_API_KEY is not set")
        return cls(AsyncOpenAI(api_key=settings.openai_api_key), settings.intent_model)

    async def plan(self, state: TurnState) -> ActionDecision:
        completion = await self._client.chat.completions.parse(
            model=self._model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": _user_message(state)},
            ],
            response_format=ActionDecision,
        )
        parsed = completion.choices[0].message.parsed
        if not isinstance(parsed, ActionDecision):
            raise ActionPlannerError("model returned no structured action")
        return parsed


def records_from_decision(call_id: str, turn_id: int, decision: ActionDecision) -> list[ToolCallRecord]:
    return [
        ToolCallRecord(
            tool=choice.tool.value,
            args=choice.args,
            idempotency_key=f"{call_id}:{turn_id}:{choice.tool.value}:{index}",
        )
        for index, choice in enumerate(decision.tool_calls)
    ]


def _user_message(state: TurnState) -> str:
    intent = state.classified_intent.value if isinstance(state.classified_intent, Intent) else "unset"
    facts = state.call_facts
    if facts is None:
        facts_text = "(none)"
    else:
        medicines = [item.name for item in facts.cart_snapshot]
        facts_text = (
            f"segment={facts.user_segment}; "
            f"applied_offer_id={facts.applied_offer_id}; "
            f"payment_link_sent={facts.payment_link_sent}; "
            f"confirmed_address={facts.confirmed_address is not None}; "
            f"cart={', '.join(medicines) if medicines else '(empty)'}"
        )
    recent = [
        f"turn {turn.turn_id}: intent={turn.classified_intent} said={turn.user_raw_transcript}"
        for turn in state.recent_turns
    ]
    recent_text = "\n".join(recent) if recent else "(none)"
    return (
        f"classified_intent: {intent}\n"
        f"Caller said: {state.user_transcript}\n"
        f"call_facts: {facts_text}\n"
        f"Recent turns:\n{recent_text}"
    )
