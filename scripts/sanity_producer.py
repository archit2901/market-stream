import asyncio
import json
from datetime import datetime, timezone

from aiokafka import AIOKafkaProducer


async def main() -> None:
    producer = AIOKafkaProducer(
        bootstrap_servers="localhost:9092",
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    await producer.start()
    try:
        for i in range(10):
            payload = {
                "seq": i,
                "ts": datetime.now(timezone.utc).isoformat(),
                "message": f"hello kafka {i}",
            }
            await producer.send_and_wait("sanity.test", key=f"key-{i}", value=payload)
            print(f"sent {payload}")
            await asyncio.sleep(0.5)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())