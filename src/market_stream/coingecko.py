from datetime import datetime, timezone
from decimal import Decimal

import httpx
import structlog

from market_stream.config import get_settings
from market_stream.schemas import CryptoRawMessage

log = structlog.get_logger()


# Map CoinGecko asset IDs to human-readable symbols.
# CoinGecko has its own naming ('ripple' for XRP, 'polygon-ecosystem-token' for
# POL) that we don't want leaking into our normalized schema later.
ASSET_ID_TO_SYMBOL: dict[str, str] = {
    "bitcoin": "BTC",
    "ethereum": "ETH",
    "solana": "SOL",
    "ripple": "XRP",
    "dogecoin": "DOGE",
    "cardano": "ADA",
    "chainlink": "LINK",
}


class CoinGeckoError(Exception):
    """Raised when CoinGecko returns an error or malformed response."""


class CoinGeckoClient:
    """Thin async client around CoinGecko's /simple/price endpoint.

    Owns exactly one HTTP session and knows exactly one endpoint. Callers hand
    it a list of asset IDs, it hands back typed `CryptoRawMessage` objects.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client
        self._settings = get_settings()

    async def fetch_prices(self, asset_ids: list[str]) -> list[CryptoRawMessage]:
        """Fetch current USD prices for the given asset IDs.

        Returns one message per asset ID that CoinGecko returned data for.
        Missing assets in the response are logged and skipped, not raised —
        one unrecognized ticker shouldn't kill the whole poll.
        """
        params = {
            "ids": ",".join(asset_ids),
            "vs_currencies": "usd",
            "include_last_updated_at": "true",
        }

        try:
            response = await self._client.get(
                f"{self._settings.coingecko_base_url}/simple/price",
                params=params,
                timeout=self._settings.coingecko_request_timeout_seconds,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise CoinGeckoError(f"request failed: {exc}") from exc

        payload = response.json()
        ingestion_ts = datetime.now(timezone.utc)

        messages: list[CryptoRawMessage] = []
        for asset_id in asset_ids:
            asset_data = payload.get(asset_id)
            if asset_data is None:
                log.warning("asset missing from response", asset_id=asset_id)
                continue

            try:
                price = Decimal(str(asset_data["usd"]))
                source_ts = datetime.fromtimestamp(
                    asset_data["last_updated_at"], tz=timezone.utc
                )
            except (KeyError, TypeError, ValueError) as exc:
                log.warning(
                    "malformed asset data",
                    asset_id=asset_id,
                    data=asset_data,
                    error=str(exc),
                )
                continue

            symbol = ASSET_ID_TO_SYMBOL.get(asset_id, asset_id.upper())

            messages.append(
                CryptoRawMessage(
                    asset_id=asset_id,
                    symbol=symbol,
                    price_usd=price,
                    source_timestamp=source_ts,
                    ingestion_timestamp=ingestion_ts,
                )
            )

        return messages