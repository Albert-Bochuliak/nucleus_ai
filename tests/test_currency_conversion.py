from decimal import Decimal

import pytest

from app.streams import CONSUMER_GROUP_NAME, STREAM_NAME
from app.worker import Worker


class FakeRedis:
    def __init__(self) -> None:
        self.acks: list[tuple[str, str, str]] = []

    async def xack(self, stream_name: str, group_name: str, message_id: str) -> int:
        self.acks.append((stream_name, group_name, message_id))
        return 1


class FakeDedupService:
    def __init__(self, already_processed: bool = False) -> None:
        self.already_processed = already_processed
        self.marked_ids: list[str] = []

    async def is_processed(self, _event_id: str) -> bool:
        return self.already_processed

    async def mark_processed(self, event_id: str) -> None:
        self.marked_ids.append(event_id)


class FakeFxService:
    async def get_rate(self, _currency: str) -> Decimal:
        return Decimal("1.25")


class FakeTransactionService:
    def __init__(self) -> None:
        self.converted_amounts: list[Decimal] = []

    async def insert_converted(self, _event, amount_usd: Decimal) -> bool:
        self.converted_amounts.append(amount_usd)
        return True


@pytest.mark.asyncio
async def test_worker_converts_amount_to_usd_and_acknowledges_message() -> None:
    redis = FakeRedis()
    dedup = FakeDedupService(already_processed=False)
    fx = FakeFxService()
    transactions = FakeTransactionService()
    worker = Worker(redis=redis, dedup_service=dedup, fx_service=fx, transaction_service=transactions)

    fields = {
        "id": "evt-1",
        "user_id": "user-1",
        "amount": "10",
        "currency": "EUR",
        "timestamp": "2026-06-08T00:00:00Z",
    }

    await worker._process_with_retry("1-0", fields)

    assert transactions.converted_amounts == [Decimal("12.50")]
    assert dedup.marked_ids == ["evt-1"]
    assert redis.acks == [(STREAM_NAME, CONSUMER_GROUP_NAME, "1-0")]
