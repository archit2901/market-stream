from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Application configuration, loaded from environment variables.

    Any variable defined here can be overridden by setting an env var of the
    same name (case-insensitive). A `.env` file in the repo root is also read
    automatically.
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Kafka
    kafka_bootstrap_servers: str = Field(default="localhost:9092")
    kafka_crypto_raw_topic: str = Field(default="crypto.raw")

    # CoinGecko
    coingecko_base_url: str = Field(default="https://api.coingecko.com/api/v3")
    coingecko_poll_interval_seconds: float = Field(default=15.0)
    coingecko_request_timeout_seconds: float = Field(default=10.0)
    coingecko_asset_ids: list[str] = Field(
        default_factory=lambda: [
            "bitcoin",
            "ethereum",
            "solana",
            "ripple",
            "dogecoin",
            "cardano",
            "chainlink",
        ]
    )

    # Logging
    log_level: str = Field(default="INFO")
    log_format: str = Field(default="json", description="'json' or 'console'")

    # Finnhub
    finnhub_api_key: str = Field(default="")
    finnhub_base_url: str = Field(default="https://finnhub.io/api/v1")
    finnhub_poll_interval_seconds: float = Field(default=30.0)
    finnhub_request_timeout_seconds: float = Field(default=10.0)
    finnhub_symbols: list[str] = Field(
        default_factory=lambda: ["AAPL", "MSFT", "NVDA", "JPM", "TSLA", "WMT", "XOM"]
    )

    # Kafka topics — add the two new ones
    kafka_stocks_raw_topic: str = Field(default="stocks.raw")
    kafka_prices_normalized_topic: str = Field(default="prices.normalized")


@lru_cache
def get_settings() -> Settings:
    """Return the (cached) settings instance.

    Cached because parsing env vars every call is wasteful, and because
    settings shouldn't change at runtime.
    """
    return Settings()