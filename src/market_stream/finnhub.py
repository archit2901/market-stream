import asyncio
from datetime import datetime, timezone
from decimal import Decimal

import httpx
import structlog

from market_stream.config import get_settings
from market_stream.schemas import StockRawMessage

log = structlog.get_logger()


class FinnhubError(Exception):
    """Raised when Finnhub returns an error or malformed response."""


class FinnhubClient:
    """Thin async client around Finnhub's /quote endpoint.

    Finnhub's free tier requires one request per symbol — no batch endpoint.
    This client fetches them concurrently with asyncio.gather to keep total
    poll latency roughly equal to a single request, not N×.
    """

    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client
        self._settings = get_settings()

    async def fetch_quotes(self, symbols: list[str]) -> list[StockRawMessage]:
        """Fetch current quotes for the given symbols concurrently.

        Individual symbol failures log-and-skip rather than raising, matching
        the CoinGecko client's contract.
        """
        results = await asyncio.gather(
            *(self._fetch_one(symbol) for symbol in symbols),
            return_exceptions=True,
        )

        messages: list[StockRawMessage] = []
        for symbol, result in zip(symbols, results):
            if isinstance(result, BaseException):
                log.warning("quote fetch failed", symbol=symbol, error=str(result))
                continue
            if result is None:
                # _fetch_one already logged the specific reason.
                continue
            messages.append(result)

        return messages

    async def _fetch_one(self, symbol: str) -> StockRawMessage | None:
        params = {"symbol": symbol, "token": self._settings.finnhub_api_key}
        try:
            response = await self._client.get(
                f"{self._settings.finnhub_base_url}/quote",
                params=params,
                timeout=self._settings.finnhub_request_timeout_seconds,
            )
            response.raise_for_status()
        except httpx.HTTPError as exc:
            raise FinnhubError(f"request failed for {symbol}: {exc}") from exc

        payload = response.json()

        # Finnhub returns zeros for unknown symbols instead of a 4xx.
        # `t == 0` is the reliable "no data" indicator.
        if payload.get("t", 0) == 0:
            log.warning("no data returned for symbol", symbol=symbol, payload=payload)
            return None

        ingestion_ts = datetime.now(timezone.utc)
        try:
            return StockRawMessage(
                symbol=symbol,
                price_usd=Decimal(str(payload["c"])),
                previous_close=Decimal(str(payload["pc"])),
                day_high=Decimal(str(payload["h"])),
                day_low=Decimal(str(payload["l"])),
                day_open=Decimal(str(payload["o"])),
                source_timestamp=datetime.fromtimestamp(payload["t"], tz=timezone.utc),
                ingestion_timestamp=ingestion_ts,
            )
        except (KeyError, TypeError, ValueError) as exc:
            log.warning(
                "malformed quote data",
                symbol=symbol,
                payload=payload,
                error=str(exc),
            )
            return None