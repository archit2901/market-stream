import asyncio

import httpx
import structlog

from market_stream.config import get_settings
from market_stream.finnhub import FinnhubClient
from market_stream.logging_setup import configure_logging


async def main() -> None:
    configure_logging()
    log = structlog.get_logger()

    settings = get_settings()
    if not settings.finnhub_api_key:
        log.error("FINNHUB_API_KEY is not set — check your .env file")
        return

    async with httpx.AsyncClient() as http:
        client = FinnhubClient(http)
        messages = await client.fetch_quotes(settings.finnhub_symbols)

    log.info("fetched quotes", count=len(messages))
    for msg in messages:
        log.info(
            "quote",
            symbol=msg.symbol,
            price_usd=str(msg.price_usd),
            previous_close=str(msg.previous_close),
            source_ts=msg.source_timestamp.isoformat(),
            ingestion_ts=msg.ingestion_timestamp.isoformat(),
        )


if __name__ == "__main__":
    asyncio.run(main())