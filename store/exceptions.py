class StoreError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class NotFoundError(StoreError):
    pass


class ConflictError(StoreError):
    pass


class ValidationError(StoreError):
    pass
