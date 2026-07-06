import asyncio
import signal
from typing import Any

import httpx
import structlog
from aiokafka import AIOKafkaProducer

from market_stream.coingecko import CoinGeckoClient, CoinGeckoError
from market_stream.config import get_settings
from market_stream.logging_setup import configure_logging
from market_stream.schemas import CryptoRawMessage

log = structlog.get_logger()


class CryptoProducer:
    """Long-running producer that polls CoinGecko and publishes to `crypto.raw`.

    Owns the polling schedule, the Kafka producer, and the shutdown lifecycle.
    The CoinGecko client is injected so tests can substitute a fake.
    """

    def __init__(
        self,
        coingecko: CoinGeckoClient,
        kafka_producer: AIOKafkaProducer,
    ) -> None:
        self._coingecko = coingecko
        self._producer = kafka_producer
        self._settings = get_settings()
        self._shutdown_event = asyncio.Event()

    def request_shutdown(self) -> None:
        """Signal the poll loop to exit cleanly after the current iteration."""
        log.info("shutdown requested")
        self._shutdown_event.set()

    async def run(self) -> None:
        """Poll → publish → sleep, until shutdown is requested."""
        log.info(
            "producer starting",
            assets=self._settings.coingecko_asset_ids,
            interval_s=self._settings.coingecko_poll_interval_seconds,
            topic=self._settings.kafka_crypto_raw_topic,
        )

        while not self._shutdown_event.is_set():
            await self._poll_and_publish_once()
            await self._sleep_or_shutdown()

        log.info("poll loop exited")

    async def _poll_and_publish_once(self) -> None:
        try:
            messages = await self._coingecko.fetch_prices(
                self._settings.coingecko_asset_ids
            )
        except CoinGeckoError as exc:
            # Transient upstream failures should not crash the producer.
            log.warning("poll failed, will retry next tick", error=str(exc))
            return

        for message in messages:
            await self._publish(message)

        log.info("poll complete", published=len(messages))

    async def _publish(self, message: CryptoRawMessage) -> None:
        # Key on symbol so all messages for the same asset land in the same
        # partition, preserving per-symbol ordering.
        key = message.symbol.encode("utf-8")
        value = message.model_dump_json().encode("utf-8")

        await self._producer.send_and_wait(
            self._settings.kafka_crypto_raw_topic,
            key=key,
            value=value,
        )

    async def _sleep_or_shutdown(self) -> None:
        """Sleep for the poll interval, or wake early if shutdown was signaled."""
        try:
            await asyncio.wait_for(
                self._shutdown_event.wait(),
                timeout=self._settings.coingecko_poll_interval_seconds,
            )
        except asyncio.TimeoutError:
            # Normal case: interval elapsed without shutdown.
            pass


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, producer: CryptoProducer) -> None:
    """Wire SIGINT and SIGTERM to trigger graceful shutdown."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, producer.request_shutdown)


async def main() -> None:
    configure_logging()
    settings = get_settings()

    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        client_id="market-stream-crypto-producer",
    )
    await kafka_producer.start()

    async with httpx.AsyncClient() as http:
        coingecko = CoinGeckoClient(http)
        producer = CryptoProducer(coingecko, kafka_producer)

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