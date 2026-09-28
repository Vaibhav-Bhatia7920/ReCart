from typing import Protocol

from openai import AsyncOpenAI

from agent.errors import IntentClassifierError
from agent.intents import Intent, IntentClassification
from agent.state import TurnState
from app.settings import get_settings

_SYSTEM = (
    "Classify the caller's latest utterance as exactly one intent. "
    f"Allowed intents: {', '.join(intent.value for intent in Intent)}. "
    "Use clinical_question only for symptoms, dosing, side effects, interactions, "
    "or appropriateness questions about prescription, restricted, or otherwise "
    "medicated drugs that a pharmacist must handle. "
    "Do not use clinical_question for vitamins, supplements, or typical OTC products; "
    "those stay on the agent path (usually other). "
    "The response schema is the only output."
)


class IntentClassifier(Protocol):
    async def classify(self, state: TurnState) -> IntentClassification: ...


class OpenAIIntentClassifier:
    """Calls the model with IntentClassification as the response schema."""

    def __init__(self, client: AsyncOpenAI, model: str) -> None:
        self._client = client
        self._model = model

    @classmethod
    def from_settings(cls) -> "OpenAIIntentClassifier":
        settings = get_settings()
        if not settings.openai_api_key:
            raise IntentClassifierError("OPENAI_API_KEY is not set")
        return cls(AsyncOpenAI(api_key=settings.openai_api_key), settings.intent_model)

    async def classify(self, state: TurnState) -> IntentClassification:
        completion = await self._client.chat.completions.parse(
            model=self._model,
            messages=[
                {"role": "system", "content": _SYSTEM},
                {"role": "user", "content": _user_message(state)},
            ],
            response_format=IntentClassification,
        )
        parsed = completion.choices[0].message.parsed
        if not isinstance(parsed, IntentClassification):
            raise IntentClassifierError("model returned no structured intent")
        return parsed


def _user_message(state: TurnState) -> str:
    medicines: list[str] = []
    if state.call_facts is not None:
        medicines = [item.name for item in state.call_facts.cart_snapshot]
    recent = [f"turn {turn.turn_id}: {turn.user_raw_transcript}" for turn in state.recent_turns]
    recent_text = "\n".join(recent) if recent else "(none)"
    cart_text = ", ".join(medicines) if medicines else "(none)"
    return (
        f"Caller said: {state.user_transcript}\n"
        f"Cart medicines: {cart_text}\n"
        f"Recent turns:\n{recent_text}"
    )
