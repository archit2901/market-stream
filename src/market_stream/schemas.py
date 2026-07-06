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