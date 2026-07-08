import asyncio
import signal

import httpx
import structlog
from aiokafka import AIOKafkaProducer

from market_stream.config import get_settings
from market_stream.finnhub import FinnhubClient, FinnhubError
from market_stream.logging_setup import configure_logging
from market_stream.schemas import StockRawMessage

log = structlog.get_logger()


class StockProducer:
    """Long-running producer that polls Finnhub and publishes to `stocks.raw`.

    Same shape as CryptoProducer — the two are deliberately kept parallel so
    the polling-to-stream pattern is obvious across sources.
    """

    def __init__(
        self,
        finnhub: FinnhubClient,
        kafka_producer: AIOKafkaProducer,
    ) -> None:
        self._finnhub = finnhub
        self._producer = kafka_producer
        self._settings = get_settings()
        self._shutdown_event = asyncio.Event()

    def request_shutdown(self) -> None:
        log.info("shutdown requested")
        self._shutdown_event.set()

    async def run(self) -> None:
        log.info(
            "producer starting",
            symbols=self._settings.finnhub_symbols,
            interval_s=self._settings.finnhub_poll_interval_seconds,
            topic=self._settings.kafka_stocks_raw_topic,
        )

        while not self._shutdown_event.is_set():
            await self._poll_and_publish_once()
            await self._sleep_or_shutdown()

        log.info("poll loop exited")

    async def _poll_and_publish_once(self) -> None:
        try:
            messages = await self._finnhub.fetch_quotes(self._settings.finnhub_symbols)
        except FinnhubError as exc:
            log.warning("poll failed, will retry next tick", error=str(exc))
            return

        for message in messages:
            await self._publish(message)

        log.info("poll complete", published=len(messages))

    async def _publish(self, message: StockRawMessage) -> None:
        # Same partitioning strategy as crypto: symbol as key preserves per-ticker ordering.
        key = message.symbol.encode("utf-8")
        value = message.model_dump_json().encode("utf-8")

        await self._producer.send_and_wait(
            self._settings.kafka_stocks_raw_topic,
            key=key,
            value=value,
        )

    async def _sleep_or_shutdown(self) -> None:
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(),
                timeout=self._settings.finnhub_poll_interval_seconds,
            )
        except asyncio.TimeoutError:
            pass


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, producer: StockProducer) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, producer.request_shutdown)


async def main() -> None:
    configure_logging()
    settings = get_settings()

    if not settings.finnhub_api_key:
        log.error("FINNHUB_API_KEY is not set — check your .env file")
        return

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        client_id="market-stream-stock-producer",
    )
    await kafka_producer.start()

    async with httpx.AsyncClient() as http:
        finnhub = FinnhubClient(http)
        producer = StockProducer(finnhub, kafka_producer)

        loop = asyncio.get_running_loop()
        _install_signal_handlers(loop, producer)

        try:
            await producer.run()
        finally:
            log.info("flushing kafka producer")
            await kafka_producer.stop()
            log.info("clean shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())