import asyncio
import json

from aiokafka import AIOKafkaConsumer

from market_stream.config import get_settings


async def main() -> None:
    settings = get_settings()
    consumer = AIOKafkaConsumer(
        settings.kafka_stocks_raw_topic,
        bootstrap_servers=settings.kafka_bootstrap_servers,
        group_id="tail-stocks-raw",
        auto_offset_reset="latest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            if msg.value is None:
                continue
            key = msg.key or "-"
            print(
                f"[p{msg.partition}@{msg.offset:>5}] "
                f"key={key:<5} "
                f"symbol={msg.value['symbol']:<5} "
                f"price=${msg.value['price_usd']:>10} "
                f"prev_close=${msg.value['previous_close']:>10} "
                f"source_ts={msg.value['source_timestamp']}"
            )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())