"""Shared pytest fixtures for market-stream integration tests.

The Kafka container is started ONCE per test session and reused across all
tests — starting a broker takes ~15s, so per-test containers would make the
suite miserable. Tests that need topic isolation should create their own
uniquely-named topics.
"""

import asyncio
import uuid
from collections.abc import AsyncIterator, Iterator

import pytest
import pytest_asyncio
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from testcontainers.kafka import KafkaContainer


@pytest.fixture(scope="session")
def kafka_container() -> Iterator[KafkaContainer]:
    """Start a real Kafka broker in Docker for the test session."""
    with KafkaContainer("confluentinc/cp-kafka:7.6.1") as container:
        yield container


@pytest.fixture(scope="session")
def kafka_bootstrap(kafka_container: KafkaContainer) -> str:
    """Bootstrap servers URL for the session's Kafka container."""
    return kafka_container.get_bootstrap_server()


@pytest_asyncio.fixture
async def unique_topic(kafka_bootstrap: str) -> AsyncIterator[str]:
    """A freshly-created, uniquely-named topic for a single test.

    Using unique topics per test means tests don't need to worry about each
    other's messages showing up in their consumers.
    """
    topic_name = f"test.{uuid.uuid4().hex[:8]}"

    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        await admin.create_topics(
            [NewTopic(name=topic_name, num_partitions=3, replication_factor=1)]
        )
    finally:
        await admin.close()

    yield topic_name

    # Best-effort cleanup — a failing test shouldn't leave garbage topics.
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        await admin.delete_topics([topic_name])
    except Exception:
        pass
    finally:
        await admin.close()


@pytest.fixture(scope="session")
def event_loop() -> Iterator[asyncio.AbstractEventLoop]:
    """Session-scoped event loop so the Kafka container survives across tests.

    Default pytest-asyncio scoping creates a new loop per test, which would
    break our session-scoped async fixtures.
    """
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()