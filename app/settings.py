from decimal import Decimal
from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Process configuration loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_host: str = "0.0.0.0"
    app_port: int = 8000
    database_url: str
    redis_url: str
    postgres_user: str = "cart"
    postgres_password: str = "cart"
    postgres_db: str = "cart_recovery"
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    cart_abandonment_timeout_seconds: int = 1800
    cart_abandonment_sweep_interval_seconds: int = 10
    cart_abandonment_sweeper_enabled: bool = True
    redis_abandonment_stream: str = "store:cart-abandoned"
    redis_abandonment_emitted_prefix: str = "store:abandonment"
    store_delivery_fee: Decimal = Decimal("4.99")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()
