import asyncio
import json
import signal
from datetime import datetime, timezone

import structlog
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.structs import ConsumerRecord

from market_stream.config import get_settings
from market_stream.logging_setup import configure_logging
from market_stream.schemas import CryptoRawMessage, PriceTick, StockRawMessage

log = structlog.get_logger()

# An equity tick older than this (vs. ingestion time) is treated as stale.
STALE_EQUITY_THRESHOLD_SECONDS = 60.0


class Normalizer:
    """Consumes crypto.raw and stocks.raw, publishes unified PriceTicks to prices.normalized."""

    def __init__(self, consumer: AIOKafkaConsumer, producer: AIOKafkaProducer) -> None:
        self._consumer = consumer
        self._producer = producer
        self._settings = get_settings()
        self._shutdown_event = asyncio.Event()
        # Newest source_timestamp published per (asset_class, symbol), used to drop duplicates.
        self._last_source_ts: dict[tuple[str, str], datetime] = {}

    def request_shutdown(self) -> None:
        log.info("shutdown requested")
        self._shutdown_event.set()

    async def run(self) -> None:
        log.info(
            "normalizer starting",
            input_topics=[
                self._settings.kafka_crypto_raw_topic,
                self._settings.kafka_stocks_raw_topic,
            ],
            output_topic=self._settings.kafka_prices_normalized_topic,
        )
        # getmany with a timeout wakes up every 0.5s, so Ctrl+C is noticed quickly
        # even when no messages are arriving.
        while not self._shutdown_event.is_set():
            batch = await self._consumer.getmany(timeout_ms=500)
            for _partition, messages in batch.items():
                for msg in messages:
                    await self._process(msg)
        log.info("consume loop exited")

    async def _process(self, msg: ConsumerRecord) -> None:
        if msg.value is None:
            return  # tombstone, nothing to normalize

        try:
            payload = json.loads(msg.value.decode("utf-8"))
            tick = self._normalize(msg.topic, payload)
        except Exception as exc:
            # One bad message is logged and skipped. It must not kill the normalizer.
            log.warning(
                "failed to normalize message",
                topic=msg.topic,
                offset=msg.offset,
                error=str(exc),
            )
            return

        dedupe_key = (tick.asset_class, tick.symbol)
        last_ts = self._last_source_ts.get(dedupe_key)
        if last_ts is not None and tick.source_timestamp <= last_ts:
            log.debug(
                "duplicate tick dropped",
                symbol=tick.symbol,
                source_timestamp=tick.source_timestamp.isoformat(),
            )
            return
        self._last_source_ts[dedupe_key] = tick.source_timestamp

        await self._producer.send_and_wait(
            self._settings.kafka_prices_normalized_topic,
            key=tick.symbol.encode("utf-8"),
            value=tick.model_dump_json().encode("utf-8"),
        )

    def _normalize(self, topic: str, payload: dict) -> PriceTick:
        now = datetime.now(timezone.utc)

        if topic == self._settings.kafka_crypto_raw_topic:
            raw = CryptoRawMessage.model_validate(payload)
            return PriceTick(
                symbol=raw.symbol,
                asset_class="crypto",
                price_usd=raw.price_usd,
                source="coingecko",
                source_timestamp=raw.source_timestamp,
                ingestion_timestamp=raw.ingestion_timestamp,
                normalized_timestamp=now,
                is_stale=False,  # crypto markets never close
            )

        if topic == self._settings.kafka_stocks_raw_topic:
            raw = StockRawMessage.model_validate(payload)
            lag = (raw.ingestion_timestamp - raw.source_timestamp).total_seconds()
            return PriceTick(
                symbol=raw.symbol,
                asset_class="equity",
                price_usd=raw.price_usd,
                source="finnhub",
                source_timestamp=raw.source_timestamp,
                ingestion_timestamp=raw.ingestion_timestamp,
                normalized_timestamp=now,
                is_stale=lag > STALE_EQUITY_THRESHOLD_SECONDS,
            )

        raise ValueError(f"unknown source topic: {topic}")


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, normalizer: Normalizer) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, normalizer.request_shutdown)


async def main() -> None:
    configure_logging()
    settings = get_settings()

    consumer = AIOKafkaConsumer(
        settings.kafka_crypto_raw_topic,
        settings.kafka_stocks_raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="normalizer",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        client_id="market-stream-normalizer",
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        enable_idempotence=True,
        acks="all",
        client_id="market-stream-normalizer",
    )

    await consumer.start()
    await producer.start()

    normalizer = Normalizer(consumer, producer)
    _install_signal_handlers(asyncio.get_running_loop(), normalizer)

    try:
        await normalizer.run()
    finally:
        log.info("shutting down")
        await consumer.stop()
        await producer.stop()
        log.info("clean shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())