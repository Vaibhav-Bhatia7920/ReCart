import enum
from datetime import datetime

from sqlalchemy import DateTime, Enum, ForeignKey, Identity, Integer, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from store.models import Base


class CallOutcome(str, enum.Enum):
    RESOLVED_CORRECT = "resolved_correct"
    RESOLVED_INCORRECT = "resolved_incorrect"
    ESCALATED_APPROPRIATE = "escalated_appropriate"
    ESCALATED_UNNECESSARY = "escalated_unnecessary"
    MISSED_ESCALATION = "missed_escalation"
    DROPPED = "dropped"
    IN_PROGRESS = "in_progress"


TERMINAL_OUTCOMES = frozenset(
    {
        CallOutcome.RESOLVED_CORRECT,
        CallOutcome.RESOLVED_INCORRECT,
        CallOutcome.ESCALATED_APPROPRIATE,
        CallOutcome.ESCALATED_UNNECESSARY,
        CallOutcome.MISSED_ESCALATION,
        CallOutcome.DROPPED,
    }
)


def _outcome_values(enum_cls: type[CallOutcome]) -> list[str]:
    return [member.value for member in enum_cls]


class CallFacts(Base):
    __tablename__ = "call_facts"

    call_id: Mapped[str] = mapped_column(String(128), primary_key=True)
    customer_id: Mapped[str] = mapped_column(String(128), nullable=False)
    phone_number: Mapped[str] = mapped_column(String(32), nullable=False)
    user_segment: Mapped[str] = mapped_column(String(64), nullable=False)
    cart_snapshot: Mapped[list[dict[str, object]]] = mapped_column(JSONB, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    confirmed_address: Mapped[dict[str, object] | None] = mapped_column(JSONB, nullable=True)
    applied_offer_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    payment_link_sent: Mapped[bool] = mapped_column(nullable=False, default=False, server_default="false")
    turn_facts: Mapped[list[dict[str, object]]] = mapped_column(
        JSONB, nullable=False, default=list, server_default="[]"
    )
    call_outcome: Mapped[CallOutcome] = mapped_column(
        Enum(
            CallOutcome,
            name="call_outcome",
            native_enum=True,
            values_callable=_outcome_values,
        ),
        nullable=False,
        default=CallOutcome.IN_PROGRESS,
        server_default=CallOutcome.IN_PROGRESS.value,
    )
    current_turn_index: Mapped[int] = mapped_column(Integer, nullable=False, default=0, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    turns: Mapped[list["TurnLog"]] = relationship(back_populates="call")


class TurnLog(Base):
    """One graph invocation. Telemetry stays in the telemetry writer, keyed by call_id and turn_id."""

    __tablename__ = "turn_logs"
    __table_args__ = (UniqueConstraint("call_id", "turn_id", name="uq_turn_logs_call_turn"),)

    id: Mapped[int] = mapped_column(Integer, Identity(), primary_key=True)
    call_id: Mapped[str] = mapped_column(
        String(128),
        ForeignKey("call_facts.call_id", ondelete="RESTRICT"),
        nullable=False,
    )
    turn_id: Mapped[int] = mapped_column(Integer, nullable=False)
    schema_version: Mapped[int] = mapped_column(Integer, nullable=False, default=1, server_default="1")
    user_raw_transcript: Mapped[str] = mapped_column(Text, nullable=False)
    classified_intent: Mapped[str] = mapped_column(String(128), nullable=False)
    tool_calls: Mapped[list[dict[str, object]]] = mapped_column(JSONB, nullable=False)
    agent_response_text: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )

    call: Mapped[CallFacts] = relationship(back_populates="turns")
