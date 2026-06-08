import os
from datetime import datetime
from decimal import Decimal

import asyncpg
from fastapi import Depends, FastAPI, Query, Request
from pydantic import BaseModel, Field
from redis.asyncio import Redis

from app.metrics import metrics
from app.streams import publish_event


class EventIn(BaseModel):
    id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    amount: Decimal
    currency: str = Field(min_length=3, max_length=3)
    timestamp: datetime


class UserSummaryOut(BaseModel):
    total_usd: Decimal
    count: int


class TransactionOut(BaseModel):
    id: int
    user_id: str
    amount_original: Decimal
    currency: str
    amount_usd: Decimal
    timestamp: datetime


class TransactionsPageOut(BaseModel):
    page: int
    limit: int
    total: int
    items: list[TransactionOut]


class MetricsOut(BaseModel):
    events_processed: int
    failed_events: int
    queue_lag: int


class EventService:
    def __init__(self, redis: Redis) -> None:
        # Store Redis client used to publish incoming events.
        self._redis = redis

    async def publish(self, event: EventIn) -> None:
        # Send event payload to Redis stream for async processing.
        await publish_event(event, redis=self._redis)


class UserService:
    def __init__(self, pool: asyncpg.Pool) -> None:
        # Store PostgreSQL connection pool for read queries.
        self._pool = pool

    async def get_summary(self, user_id: str) -> UserSummaryOut:
        # Fetch total USD amount and transaction count for one user.
        query = """
            SELECT
                COALESCE(SUM(amount_usd), 0) AS total_usd,
                COUNT(*)::INT AS count
            FROM transactions
            WHERE user_id = $1
        """
        async with self._pool.acquire() as connection:
            row = await connection.fetchrow(query, user_id)

        return UserSummaryOut(
            total_usd=row["total_usd"] if row else Decimal("0"),
            count=row["count"] if row else 0,
        )

    async def list_transactions(
        self,
        user_id: str,
        from_ts: datetime | None,
        to_ts: datetime | None,
        page: int,
        limit: int,
    ) -> TransactionsPageOut:
        # Return paginated transactions ordered by newest first.
        offset = (page - 1) * limit

        total_query = """
            SELECT COUNT(*)::INT AS total
            FROM transactions
            WHERE user_id = $1
              AND ($2::timestamptz IS NULL OR \"timestamp\" >= $2)
              AND ($3::timestamptz IS NULL OR \"timestamp\" <= $3)
        """
        list_query = """
            SELECT id, user_id, amount_original, currency, amount_usd, "timestamp"
            FROM transactions
            WHERE user_id = $1
              AND ($2::timestamptz IS NULL OR "timestamp" >= $2)
              AND ($3::timestamptz IS NULL OR "timestamp" <= $3)
            ORDER BY "timestamp" DESC
            LIMIT $4 OFFSET $5
        """

        async with self._pool.acquire() as connection:
            total_row = await connection.fetchrow(total_query, user_id, from_ts, to_ts)
            rows = await connection.fetch(list_query, user_id, from_ts, to_ts, limit, offset)

        items = [TransactionOut.model_validate(dict(row)) for row in rows]
        return TransactionsPageOut(
            page=page,
            limit=limit,
            total=total_row["total"] if total_row else 0,
            items=items,
        )

# Application lifespan management
async def lifespan(app: FastAPI):
    # Initialize shared DB and Redis clients on startup and close on shutdown.
    postgres_dsn = os.getenv(
        "POSTGRES_DSN",
        "postgresql://app:app@localhost:5432/events",
    )
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")

    app.state.pg_pool = await asyncpg.create_pool(dsn=postgres_dsn)
    app.state.redis = Redis.from_url(redis_url, decode_responses=True)

    try:
        yield
    finally:
        await app.state.pg_pool.close()
        await app.state.redis.aclose()


app = FastAPI(lifespan=lifespan)


def get_event_service(request: Request) -> EventService:
    # Build event service from app-level Redis client.
    return EventService(redis=request.app.state.redis)


def get_user_service(request: Request) -> UserService:
    # Build user service from app-level PostgreSQL pool.
    return UserService(pool=request.app.state.pg_pool)


@app.post("/events", status_code=202)
async def post_event(
    payload: EventIn,
    service: EventService = Depends(get_event_service),
) -> dict[str, str]:
    # Accept event and enqueue it for asynchronous worker processing.
    try:
        await service.publish(payload)
        metrics.increment_events_processed()
        return {"status": "accepted"}
    except Exception:
        metrics.increment_failed_events()
        raise


@app.get("/users/{user_id}/summary", response_model=UserSummaryOut)
async def get_user_summary(
    user_id: str,
    service: UserService = Depends(get_user_service),
) -> UserSummaryOut:
    # Return aggregated summary for the requested user.
    return await service.get_summary(user_id)


@app.get("/users/{user_id}/transactions", response_model=TransactionsPageOut)
async def get_user_transactions(
    user_id: str,
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None),
    page: int = Query(default=1, ge=1),
    limit: int = Query(default=50, ge=1, le=200),
    service: UserService = Depends(get_user_service),
) -> TransactionsPageOut:
    # Return paginated transaction history for the requested user.
    return await service.list_transactions(user_id, from_, to, page, limit)


@app.get("/metrics", response_model=MetricsOut)
async def get_metrics() -> MetricsOut:
    # Return current in-memory processing metrics.
    snapshot = metrics.snapshot()
    return MetricsOut(
        events_processed=snapshot.events_processed,
        failed_events=snapshot.failed_events,
        queue_lag=snapshot.queue_lag,
    )
