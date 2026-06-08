import os
from typing import Any

from pydantic import BaseModel
from redis.asyncio import Redis
from redis.exceptions import ResponseError


STREAM_NAME = os.getenv("REDIS_STREAM_NAME", "transactions")
CONSUMER_GROUP_NAME = os.getenv("REDIS_CONSUMER_GROUP", "transactions-consumers")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")


async def ensure_consumer_group(
    redis: Redis,
    stream_name: str = STREAM_NAME,
    group_name: str = CONSUMER_GROUP_NAME,
) -> None:
    # Create consumer group once; ignore error when it already exists.
    try:
        await redis.xgroup_create(
            name=stream_name,
            groupname=group_name,
            id="$",
            mkstream=True,
        )
    except ResponseError as exc:
        if "BUSYGROUP" not in str(exc):
            raise


def _event_to_stream_fields(event: BaseModel | dict[str, Any]) -> dict[str, str]:
    # Normalize event payload to string fields for Redis stream storage.
    if isinstance(event, BaseModel):
        payload = event.model_dump(mode="json")
    else:
        payload = event
    return {key: str(value) for key, value in payload.items()}


async def publish_event(
    event: BaseModel | dict[str, Any],
    redis: Redis | None = None,
    stream_name: str = STREAM_NAME,
    group_name: str = CONSUMER_GROUP_NAME,
) -> str:
    # Publish event to stream and return message ID.
    owns_client = redis is None
    client = redis or Redis.from_url(REDIS_URL, decode_responses=True)

    try:
        await ensure_consumer_group(client, stream_name=stream_name, group_name=group_name)
        fields = _event_to_stream_fields(event)
        message_id = await client.xadd(stream_name, fields)
        return message_id
    finally:
        if owns_client:
            await client.aclose()
