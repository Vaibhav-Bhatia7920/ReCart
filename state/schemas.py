from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from state.models import CallOutcome


class CartMedicine(BaseModel):
    """Rich cart line. Names and clinical text are required; an id alone is not enough."""

    model_config = ConfigDict(extra="ignore")

    name: str = Field(min_length=1)
    description: str = Field(min_length=1)
    use_cases: list[str]
    warnings: list[str]
    sku: str | None = None


class ConfirmedAddress(BaseModel):
    model_config = ConfigDict(extra="ignore")

    line1: str = Field(min_length=1)
    line2: str | None = None
    city: str = Field(min_length=1)
    region: str = Field(min_length=1)
    postal_code: str = Field(min_length=1)
    country: str = Field(min_length=1)


class ToolCallRecord(BaseModel):
    model_config = ConfigDict(extra="ignore")

    tool: str
    args: dict[str, Any]
    idempotency_key: str
    result: Any = None
    error: str | None = None


class TurnLogInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    turn_id: int = Field(ge=1)
    user_raw_transcript: str
    classified_intent: str
    tool_calls: list[ToolCallRecord]
    agent_response_text: str
    schema_version: int = 1


class TurnLogView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    call_id: str
    turn_id: int
    schema_version: int
    user_raw_transcript: str
    classified_intent: str
    tool_calls: list[ToolCallRecord]
    agent_response_text: str
    created_at: datetime


class CallFactsView(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    call_id: str
    customer_id: str
    phone_number: str
    user_segment: str
    cart_snapshot: list[CartMedicine]
    schema_version: int
    confirmed_address: ConfirmedAddress | None
    applied_offer_id: str | None
    payment_link_sent: bool
    call_outcome: CallOutcome
    current_turn_index: int
    created_at: datetime
    updated_at: datetime


class CallContext(BaseModel):
    """Facts plus the last two turns, oldest first, for the graph prompt."""

    facts: CallFactsView
    recent_turns: list[TurnLogView]


class CallFactsUpdate(BaseModel):
    """Partial update applied inside commit_turn. current_turn_index is not settable here."""

    model_config = ConfigDict(extra="forbid")

    customer_id: str | None = None
    phone_number: str | None = None
    user_segment: str | None = None
    cart_snapshot: list[CartMedicine] | None = None
    schema_version: int | None = None
    confirmed_address: ConfirmedAddress | None = None
    applied_offer_id: str | None = None
    payment_link_sent: bool | None = None
    call_outcome: CallOutcome | None = None

    def set_fields(self) -> dict[str, Any]:
        return self.model_dump(exclude_unset=True, mode="json")
