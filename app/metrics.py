from dataclasses import dataclass
from threading import Lock


@dataclass(slots=True)
class MetricsSnapshot:
    events_processed: int
    failed_events: int
    queue_lag: int


class InMemoryMetrics:
    def __init__(self) -> None:
        self._lock = Lock()
        self._events_processed = 0
        self._failed_events = 0
        self._queue_lag = 0

    def increment_events_processed(self, value: int = 1) -> None:
        with self._lock:
            self._events_processed += value

    def increment_failed_events(self, value: int = 1) -> None:
        with self._lock:
            self._failed_events += value

    def increment_queue_lag(self, value: int = 1) -> None:
        with self._lock:
            self._queue_lag += value

    def decrement_queue_lag(self, value: int = 1) -> None:
        with self._lock:
            self._queue_lag = max(0, self._queue_lag - value)

    def snapshot(self) -> MetricsSnapshot:
        with self._lock:
            return MetricsSnapshot(
                events_processed=self._events_processed,
                failed_events=self._failed_events,
                queue_lag=self._queue_lag,
            )


metrics = InMemoryMetrics()
