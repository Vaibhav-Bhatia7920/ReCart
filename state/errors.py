from state.models import CallOutcome


class CallStateError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class CallNotFoundError(CallStateError):
    def __init__(self, call_id: str) -> None:
        super().__init__(f"Call {call_id} was not found")
        self.call_id = call_id


class CallAlreadyExistsError(CallStateError):
    def __init__(self, call_id: str) -> None:
        super().__init__(f"Call {call_id} already exists")
        self.call_id = call_id


class StaleTurnError(CallStateError):
    """A commit lost the optimistic check on current_turn_index, or retried an existing turn."""

    def __init__(self, call_id: str, expected_turn_index: int) -> None:
        super().__init__(
            f"Stale turn for call {call_id}: expected current_turn_index {expected_turn_index}"
        )
        self.call_id = call_id
        self.expected_turn_index = expected_turn_index


class TerminalOutcomeError(CallStateError):
    """call_outcome is already terminal and cannot be replaced by a different outcome."""

    def __init__(self, call_id: str, current: CallOutcome, attempted: CallOutcome) -> None:
        super().__init__(
            f"Call {call_id} outcome is {current.value} and cannot change to {attempted.value}"
        )
        self.call_id = call_id
        self.current = current
        self.attempted = attempted
