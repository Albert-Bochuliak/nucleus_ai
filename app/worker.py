import asyncio
import os
from datetime import datetime
from decimal import Decimal

import asyncpg
import httpx
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from app.fx_service import FxService
from app.streams import CONSUMER_GROUP_NAME, STREAM_NAME, ensure_consumer_group


DEDUP_SET_KEY = os.getenv("REDIS_DEDUP_SET", "processed_event_ids")
CONSUMER_NAME = os.getenv("REDIS_CONSUMER_NAME", "worker-1")
FX_API_URL = os.getenv("FX_API_URL", "https://api.frankfurter.app")
MAX_BACKOFF_SECONDS = int(os.getenv("WORKER_MAX_BACKOFF_SECONDS", "60"))


class Event(BaseModel):
    id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    amount: Decimal
    currency: str = Field(min_length=3, max_length=3)
    timestamp: datetime


class DedupService:
    def __init__(self, redis: Redis, set_key: str = DEDUP_SET_KEY) -> None:
        self._redis = redis
        self._set_key = set_key

    async def is_processed(self, event_id: str) -> bool:
        return bool(await self._redis.sismember(self._set_key, event_id))

    async def mark_processed(self, event_id: str) -> None:
        await self._redis.sadd(self._set_key, event_id)


class TransactionService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        self._pool = pool

    async def insert_converted(self, event: Event, amount_usd: Decimal) -> bool:
        async with self._pool.acquire() as connection:
            async with connection.transaction():
                marker = await connection.fetchval(
                    """
                    INSERT INTO processed_events (id, processed_at)
                    VALUES ($1, NOW())
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    """,
                    event.id,
                )
                if marker is None:
                    return False

                await connection.execute(
                    """
                    INSERT INTO transactions (
                        user_id,
                        amount_original,
                        currency,
                        amount_usd,
                        "timestamp"
                    )
                    VALUES ($1, $2, $3, $4, $5)
                    """,
                    event.user_id,
                    event.amount,
                    event.currency.upper(),
                    amount_usd,
                    event.timestamp,
                )
        return True


class Worker:
    def __init__(
        self,
        redis: Redis,
        dedup_service: DedupService,
        fx_service: FxService,
        transaction_service: TransactionService,
    ) -> None:
        self._redis = redis
        self._dedup_service = dedup_service
        self._fx_service = fx_service
        self._transaction_service = transaction_service

    async def run(self) -> None:
        await ensure_consumer_group(
            self._redis,
            stream_name=STREAM_NAME,
            group_name=CONSUMER_GROUP_NAME,
        )

        while True:
            pending = await self._redis.xreadgroup(
                groupname=CONSUMER_GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: "0"},
                count=10,
            )
            if pending:
                await self._handle_batch(pending)
                continue

            messages = await self._redis.xreadgroup(
                groupname=CONSUMER_GROUP_NAME,
                consumername=CONSUMER_NAME,
                streams={STREAM_NAME: ">"},
                count=10,
                block=5000,
            )
            if messages:
                await self._handle_batch(messages)

    async def _handle_batch(self, messages: list[tuple[str, list[tuple[str, dict[str, str]]]]]) -> None:
        for _, stream_messages in messages:
            for message_id, fields in stream_messages:
                await self._process_with_retry(message_id, fields)

    async def _process_with_retry(self, message_id: str, fields: dict[str, str]) -> None:
        event = Event.model_validate(fields)

        if await self._dedup_service.is_processed(event.id):
            await self._redis.xack(STREAM_NAME, CONSUMER_GROUP_NAME, message_id)
            return

        attempt = 0
        while True:
            try:
                rate = await self._fx_service.get_rate(event.currency)
                amount_usd = event.amount * rate

                await self._transaction_service.insert_converted(event, amount_usd)
                await self._dedup_service.mark_processed(event.id)
                await self._redis.xack(STREAM_NAME, CONSUMER_GROUP_NAME, message_id)
                return
            except Exception:
                delay = min(2**attempt, MAX_BACKOFF_SECONDS)
                attempt += 1
                await asyncio.sleep(delay)


async def main() -> None:
    postgres_dsn = os.getenv(
        "POSTGRES_DSN",
        "postgresql://app:app@localhost:5432/events",
    )
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    pool = await asyncpg.create_pool(dsn=postgres_dsn)
    redis = Redis.from_url(redis_url, decode_responses=True)
    fx_client = httpx.AsyncClient(base_url=FX_API_URL)

    worker = Worker(
        redis=redis,
        dedup_service=DedupService(redis),
        fx_service=FxService(fx_client),
        transaction_service=TransactionService(pool),
    )

    try:
        await worker.run()
    finally:
        await fx_client.aclose()
        await pool.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
