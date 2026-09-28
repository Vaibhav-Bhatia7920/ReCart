from enum import Enum

from pydantic import BaseModel, ConfigDict


class Intent(str, Enum):
    """Fixed labels the classifier may return.

    clinical_question is the escalation boundary for prescription or restricted
    medicines. Routing reads ALWAYS_ESCALATE; the prompt does not decide
    whether to escalate. OTC and vitamin questions stay on the agent path.
    """

    ASK_DISCOUNT = "ask_discount"
    CONFIRM_ADDRESS = "confirm_address"
    PAYMENT_ISSUE = "payment_issue"
    CANCEL = "cancel"
    CLINICAL_QUESTION = "clinical_question"
    OTHER = "other"


ALWAYS_ESCALATE: frozenset[Intent] = frozenset({Intent.CLINICAL_QUESTION})


class IntentClassification(BaseModel):
    """Schema forced on the intent model call. The client parses into this type."""

    model_config = ConfigDict(extra="forbid")

    intent: Intent
