import asyncio
import json

from aiokafka import AIOKafkaConsumer

from market_stream.config import get_settings


async def main() -> None:
    settings = get_settings()
    consumer = AIOKafkaConsumer(
        settings.kafka_prices_normalized_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="tail-prices-normalized",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    )
    await consumer.start()
    try:
        async for msg in consumer:
            if msg.value is None:
                continue
            v = msg.value
            stale = " [STALE]" if v.get("is_stale") else ""
            print(
                f"[p{msg.partition}@{msg.offset:>5}] "
                f"{v['asset_class']:<6} {v['symbol']:<5} "
                f"${v['price_usd']:>12} src={v['source']:<9}{stale}"
            )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())