"""Agent call_facts and turn_logs.

Revision ID: 0003
Revises: 0002
Create Date: 2026-09-27
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "0003"
down_revision: str | None = "0002"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_CALL_OUTCOME = postgresql.ENUM(
    "resolved_correct",
    "resolved_incorrect",
    "escalated_appropriate",
    "escalated_unnecessary",
    "missed_escalation",
    "dropped",
    "in_progress",
    name="call_outcome",
)


def upgrade() -> None:
    _CALL_OUTCOME.create(op.get_bind(), checkfirst=False)
    op.create_table(
        "call_facts",
        sa.Column("call_id", sa.String(length=128), nullable=False),
        sa.Column("customer_id", sa.String(length=128), nullable=False),
        sa.Column("phone_number", sa.String(length=32), nullable=False),
        sa.Column("user_segment", sa.String(length=64), nullable=False),
        sa.Column("cart_snapshot", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("schema_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("confirmed_address", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("applied_offer_id", sa.String(length=128), nullable=True),
        sa.Column("payment_link_sent", sa.Boolean(), server_default=sa.text("false"), nullable=False),
        sa.Column(
            "call_outcome",
            postgresql.ENUM(name="call_outcome", create_type=False),
            server_default=sa.text("'in_progress'"),
            nullable=False,
        ),
        sa.Column("current_turn_index", sa.Integer(), server_default="0", nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.PrimaryKeyConstraint("call_id"),
    )
    op.create_table(
        "turn_logs",
        sa.Column("id", sa.Integer(), sa.Identity(), nullable=False),
        sa.Column("call_id", sa.String(length=128), nullable=False),
        sa.Column("turn_id", sa.Integer(), nullable=False),
        sa.Column("schema_version", sa.Integer(), server_default="1", nullable=False),
        sa.Column("user_raw_transcript", sa.Text(), nullable=False),
        sa.Column("classified_intent", sa.String(length=128), nullable=False),
        sa.Column("tool_calls", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("agent_response_text", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=False),
        sa.ForeignKeyConstraint(["call_id"], ["call_facts.call_id"], ondelete="RESTRICT"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("call_id", "turn_id", name="uq_turn_logs_call_turn"),
    )


def downgrade() -> None:
    op.drop_table("turn_logs")
    op.drop_table("call_facts")
    _CALL_OUTCOME.drop(op.get_bind(), checkfirst=False)
