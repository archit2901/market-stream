import asyncio
import json
import signal

import redis.asyncio as redis
import structlog
from aiokafka import AIOKafkaConsumer
from aiokafka.structs import ConsumerRecord
from pydantic import ValidationError

from market_stream.aggregation import MAX_WINDOW, AggregatorState
from market_stream.config import get_settings
from market_stream.logging_setup import configure_logging
from market_stream.schemas import AssetSnapshot, PriceTick

log = structlog.get_logger()

ASSETS_KEY = "assets"


class RedisSink:
    """Writes aggregator output to Redis. Every write is idempotent, so replays are safe."""

    def __init__(self, client: redis.Redis) -> None:
        self._redis = client

    async def write(self, snapshot: AssetSnapshot) -> None:
        asset_id = f"{snapshot.asset_class}:{snapshot.symbol}"
        history_key = f"history:{asset_id}"

        ts = snapshot.latest_source_timestamp.timestamp()  # epoch seconds
        cutoff = ts - MAX_WINDOW.total_seconds()
        point = json.dumps(
            {"t": snapshot.latest_source_timestamp.isoformat(), "p": str(snapshot.latest_price)}
        )

        # One atomic transaction: readers never see new stats with old history.
        async with self._redis.pipeline(transaction=True) as pipe:
            pipe.set(f"stats:{asset_id}", snapshot.model_dump_json())
            pipe.zadd(history_key, {point: ts})
            pipe.zremrangebyscore(history_key, "-inf", f"({cutoff}")  # drop points older than 1h
            pipe.sadd(ASSETS_KEY, asset_id)
            await pipe.execute()


class Aggregator:
    """Consumes prices.normalized, maintains rolling windows, writes snapshots to Redis."""

    def __init__(self, consumer: AIOKafkaConsumer, sink: RedisSink) -> None:
        self._consumer = consumer
        self._sink = sink
        self._state = AggregatorState()
        self._shutdown_event = asyncio.Event()

    def request_shutdown(self) -> None:
        log.info("shutdown requested")
        self._shutdown_event.set()

    async def run(self) -> None:
        log.info("aggregator starting", topic=get_settings().kafka_prices_normalized_topic)
        while not self._shutdown_event.is_set():
            batch = await self._consumer.getmany(timeout_ms=500)
            for _tp, messages in batch.items():
                for msg in messages:
                    await self._process(msg)
        log.info("consume loop exited")

    async def _process(self, msg: ConsumerRecord) -> None:
        if msg.value is None:
            return

        try:
            tick = PriceTick.model_validate_json(msg.value)
        except ValidationError as exc:
            log.warning("invalid tick skipped", offset=msg.offset, error=str(exc))
            return

        snapshot = self._state.apply(tick)
        if snapshot is None:
            return  # not newer than what we already have

        # If Redis is down, this raises and the aggregator stops loudly.
        # That's deliberate: silently dropping state updates would be worse.
        await self._sink.write(snapshot)

        log.info(
            "snapshot updated",
            symbol=snapshot.symbol,
            price=str(snapshot.latest_price),
            change_1h=f"{snapshot.windows['1h'].change_pct}%",
            ticks_1h=snapshot.windows["1h"].tick_count,
        )


def _install_signal_handlers(loop: asyncio.AbstractEventLoop, aggregator: Aggregator) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, aggregator.request_shutdown)


async def main() -> None:
    configure_logging()
    settings = get_settings()

    redis_client = redis.from_url(settings.redis_url)
    await redis_client.ping()  # fail fast with a clear error if Redis isn't running

    consumer = AIOKafkaConsumer(
        settings.kafka_prices_normalized_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="aggregator",
        enable_auto_commit=False,  # never commit; Checkpoint 3 explains why
        auto_offset_reset="latest",
        client_id="market-stream-aggregator",
    )
    await consumer.start()

    aggregator = Aggregator(consumer, RedisSink(redis_client))
    _install_signal_handlers(asyncio.get_running_loop(), aggregator)

    try:
        await aggregator.run()
    finally:
        log.info("shutting down")
        await consumer.stop()
        await redis_client.aclose()
        log.info("clean shutdown complete")


if __name__ == "__main__":
    asyncio.run(main())