import asyncio
import json

from aiokafka import AIOKafkaConsumer


async def main() -> None:
    consumer = AIOKafkaConsumer(
        "sanity.test",
        bootstrap_servers="localhost:9092",
        group_id="sanity-consumer",
        auto_offset_reset="earliest",
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k else None,
    )
    await consumer.start()
    try:
        async for msg in consumer:
            print(f"received key={msg.key} value={msg.value} "
                  f"partition={msg.partition} offset={msg.offset}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())