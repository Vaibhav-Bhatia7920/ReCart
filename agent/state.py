from enum import Enum

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agent.intents import Intent
from state.schemas import CallFactsView, ToolCallRecord, TurnLogView

RECENT_TURN_LIMIT = 2


class TurnOutcome(str, Enum):
    ESCALATED = "escalated"
    CONTINUING = "continuing"


class TurnState(BaseModel):
    """One graph invocation. Pydantic validates every field the nodes read and write."""

    model_config = ConfigDict(extra="forbid")

    call_id: str = Field(min_length=1)
    turn_id: int = Field(ge=1)
    user_transcript: str
    classified_intent: Intent | None = None
    call_facts: CallFactsView | None = None
    recent_turns: list[TurnLogView] = Field(default_factory=list)
    tool_calls_this_turn: list[ToolCallRecord] = Field(default_factory=list)
    agent_response_text: str = ""
    turn_outcome: TurnOutcome | None = None

    @field_validator("recent_turns")
    @classmethod
    def cap_recent_turns(cls, turns: list[TurnLogView]) -> list[TurnLogView]:
        if len(turns) > RECENT_TURN_LIMIT:
            raise ValueError(f"recent_turns is capped at the last {RECENT_TURN_LIMIT} turns")
        return turns
