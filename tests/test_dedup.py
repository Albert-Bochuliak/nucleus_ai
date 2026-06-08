import pytest

from app.worker import DedupService


class FakeRedisSet:
    def __init__(self) -> None:
        self._values: set[str] = set()

    async def sismember(self, _key: str, value: str) -> bool:
        return value in self._values

    async def sadd(self, _key: str, value: str) -> int:
        self._values.add(value)
        return 1


@pytest.mark.asyncio
async def test_dedup_service_marks_event_as_processed() -> None:
    redis = FakeRedisSet()
    service = DedupService(redis, set_key="processed_ids")

    assert await service.is_processed("evt-1") is False

    await service.mark_processed("evt-1")

    assert await service.is_processed("evt-1") is True
