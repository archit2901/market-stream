from datetime import datetime
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, Field


class CryptoRawMessage(BaseModel):
    """A single crypto price observation, as published to the `crypto.raw` topic.

    This is the *raw* schema — it preserves source-specific fields intentionally.
    The Phase 2 normalizer will project this into a unified schema for
    `prices.normalized`.
    """

    source: Literal["coingecko"] = "coingecko"
    asset_id: str = Field(..., description="CoinGecko's asset ID, e.g. 'bitcoin'")
    symbol: str = Field(..., description="Human-readable symbol, e.g. 'BTC'")
    price_usd: Decimal = Field(..., description="USD price at source_timestamp")
    source_timestamp: datetime = Field(
        ..., description="When CoinGecko believes the price was accurate"
    )
    ingestion_timestamp: datetime = Field(
        ..., description="When our producer polled the API"
    )

class StockRawMessage(BaseModel):
    """A single equity quote observation, as published to the `stocks.raw` topic.

    Same design as CryptoRawMessage: preserve source-specific fields intentionally,
    the normalizer will project this to the unified schema later.
    """

    source: Literal["finnhub"] = "finnhub"
    symbol: str = Field(..., description="Ticker symbol, e.g. 'AAPL'")
    price_usd: Decimal = Field(..., description="Current price at source_timestamp")
    previous_close: Decimal = Field(..., description="Previous session's close")
    day_high: Decimal
    day_low: Decimal
    day_open: Decimal
    source_timestamp: datetime = Field(
        ..., description="When the exchange generated this quote"
    )
    ingestion_timestamp: datetime = Field(
        ..., description="When our producer polled the API"
    )

class PriceTick(BaseModel):
    """Unified price observation published to `prices.normalized`.

    This is the interface contract for the entire downstream pipeline.
    Both CryptoRawMessage and StockRawMessage are projected into this shape
    by the normalizer.
    """

    symbol: str = Field(..., description="Ticker symbol, e.g. 'BTC' or 'AAPL'")
    asset_class: Literal["crypto", "equity"]
    price_usd: Decimal
    source: Literal["coingecko", "finnhub"]

    source_timestamp: datetime = Field(
        ..., description="When the upstream source believed the price was accurate"
    )
    ingestion_timestamp: datetime = Field(
        ..., description="When our producer polled the upstream API"
    )
    normalized_timestamp: datetime = Field(
        ..., description="When the normalizer processed this tick"
    )

    is_stale: bool = Field(
        default=False,
        description="True when the price is known to be old, e.g. equities outside market hours",
    )