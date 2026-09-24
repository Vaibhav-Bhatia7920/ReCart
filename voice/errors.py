
class ProviderError(Exception):
    """
    ProviderError is an exception that is raised when a provider error occurs.
    """
    def __init__(self, message: str, provider: str):
        self.message = message
        self.provider = provider
        super().__init__(self.message)


class ProviderTimeoutError(ProviderError):
    """
    ProviderTimeoutError is an exception that is raised when a provider timeout occurs.
    """
    def __init__(self, message: str, provider: str):
        super().__init__(message, provider)

class ProviderConnectionError(ProviderError):
    """
    ProviderConnectionError is an exception that is raised when a provider connection error occurs.
    """
    def __init__(self, message: str, provider: str):
        super().__init__(message, provider)

class ProviderAuthenticationError(ProviderError):
    """
    ProviderAuthenticationError is an exception that is raised when a provider authentication error occurs.
    """
    def __init__(self, message: str, provider: str):
        super().__init__(message, provider)

class ProviderRateLimitError(ProviderError):
    """
    ProviderRateLimitError is an exception that is raised when a provider rate limit error occurs.
    """
    def __init__(self, message: str, provider: str):
        super().__init__(message, provider)

class ProviderInvalidRequestError(ProviderError):
    """
    ProviderInvalidRequestError is an exception that is raised when a provider invalid request error occurs.
    """
    def __init__(self, message: str, provider: str):       
        super().__init__(message, provider)

class ASRProviderError(ProviderError):
    """Raised when an ASR provider fails. Subclasses distinguish timeout from a dropped socket."""

    def __init__(self, message: str, provider: str = "asr"):
        super().__init__(message, provider)


class ASRTimeoutError(ASRProviderError):
    """Raised when the ASR provider does not connect or respond within the timeout."""

    def __init__(self, message: str, provider: str = "asr"):
        super().__init__(message, provider)


class ASRConnectionDropped(ASRProviderError):
    """Raised when the ASR socket drops and one reconnect does not restore the stream."""

    def __init__(self, message: str, provider: str = "asr"):
        super().__init__(message, provider)


class DataCorruptionError(Exception):
    """
    DataCorruptionError is an exception that is raised when data corruption occurs.
    """
    def __init__(self, message: str):
        self.message = message
        super().__init__(self.message)
