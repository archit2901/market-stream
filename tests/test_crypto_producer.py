"""Integration tests for the crypto producer — uses a real Kafka broker
via testcontainers, but a fake CoinGecko client so tests are deterministic.
"""

import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal

import pytest
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

from market_stream.producers.crypto import CryptoProducer
from market_stream.schemas import CryptoRawMessage


class FakeCoinGeckoClient:
    """Test double — returns a fixed set of messages, records call count."""

    def __init__(self, messages: list[CryptoRawMessage]) -> None:
        self._messages = messages
        self.call_count = 0

    async def fetch_prices(self, asset_ids: list[str]) -> list[CryptoRawMessage]:
        self.call_count += 1
        return self._messages


def _make_msg(symbol: str, price: str) -> CryptoRawMessage:
    now = datetime.now(timezone.utc)
    return CryptoRawMessage(
        asset_id=symbol.lower(),
        symbol=symbol,
        price_usd=Decimal(price),
        source_timestamp=now,
        ingestion_timestamp=now,
    )


@pytest.fixture(autouse=True)
def override_settings(monkeypatch: pytest.MonkeyPatch, kafka_bootstrap: str) -> None:
    """Point Settings at the test container and shorten the poll interval."""
    from market_stream.config import get_settings

    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", kafka_bootstrap)
    monkeypatch.setenv("COINGECKO_POLL_INTERVAL_SECONDS", "0.1")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


async def test_producer_publishes_messages_to_topic(
    kafka_bootstrap: str, unique_topic: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KAFKA_CRYPTO_RAW_TOPIC", unique_topic)
    from market_stream.config import get_settings
    get_settings.cache_clear()

    fake = FakeCoinGeckoClient([_make_msg("BTC", "64000.00"), _make_msg("ETH", "3400.00")])
    kafka_producer = AIOKafkaProducer(
        bootstrap_servers=kafka_bootstrap, enable_idempotence=True
    )
    await kafka_producer.start()

    producer = CryptoProducer(fake, kafka_producer)  # type: ignore[arg-type]

    # Run the producer for just long enough to publish one poll's worth.
    async def stop_soon() -> None:
        await asyncio.sleep(0.3)
        producer.request_shutdown()

    try:
        await asyncio.gather(producer.run(), stop_soon())
    finally:
        await kafka_producer.stop()

    # Consume from the topic and assert we got the expected messages.
    consumer = AIOKafkaConsumer(
        unique_topic,
        bootstrap_servers=kafka_bootstrap,
        group_id="test-consumer",
        auto_offset_reset="earliest",
    )
    await consumer.start()
    received: list[dict] = []
    try:
        end_time = asyncio.get_event_loop().time() + 5.0
        while asyncio.get_event_loop().time() < end_time and len(received) < 2:
            batch = await consumer.getmany(timeout_ms=500)
            for tp, msgs in batch.items():
                for msg in msgs:
                    received.append(
                        {
                            "key": msg.key.decode() if msg.key else None,
                            "value": json.loads(msg.value.decode()),
                            "partition": msg.partition,
                        }
                    )
    finally:
        await consumer.stop()

    assert len(received) >= 2, f"expected at least 2 messages, got {len(received)}"
    symbols = {r["value"]["symbol"] for r in received}
    assert {"BTC", "ETH"} <= symbols

    # Same-key messages must land in the same partition.
    btc_partitions = {r["partition"] for r in received if r["key"] == "BTC"}
    assert len(btc_partitions) == 1, "BTC messages split across partitions"


async def test_producer_shuts_down_cleanly_on_signal(
    kafka_bootstrap: str, unique_topic: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KAFKA_CRYPTO_RAW_TOPIC", unique_topic)
    from market_stream.config import get_settings
    get_settings.cache_clear()

    fake = FakeCoinGeckoClient([_make_msg("BTC", "64000.00")])
    kafka_producer = AIOKafkaProducer(bootstrap_servers=kafka_bootstrap)
    await kafka_producer.start()
    producer = CryptoProducer(fake, kafka_producer)  # type: ignore[arg-type]

    run_task = asyncio.create_task(producer.run())
    await asyncio.sleep(0.2)
    producer.request_shutdown()

    # Should exit within a reasonable time, no hang.
    await asyncio.wait_for(run_task, timeout=2.0)
    await kafka_producer.stop()