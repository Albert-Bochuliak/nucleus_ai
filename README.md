# Nucleus AI Async Event Processing Service

A small async transaction ingestion and processing service built with Python, FastAPI, Redis Streams, and PostgreSQL.

## What this service does

1. Accepts transaction events over HTTP (`POST /events`).
2. Pushes events to Redis Stream queue (`transactions`).
3. Worker consumes events, deduplicates by event ID, converts amount to USD, and stores transactions in PostgreSQL.
4. Exposes read APIs for user summary, transaction listing, and basic metrics.

## Architecture and delivery semantics

### Queue choice
I used **Redis Streams** because it provides:
- consumer groups,
- pending-entry tracking,
- acknowledgment (`XACK`) semantics,
- good local developer experience with Docker.

### Failure handling
If DB or FX lookup fails, worker does **not acknowledge** the message and retries with exponential backoff. Because unacked messages remain in stream pending entries, they can be retried and are not lost.

### Delivery guarantee
This implementation uses **at-least-once** processing (not exactly-once).

- Why at-least-once: simpler and robust under transient failures.
- Duplicate protection: event IDs are deduplicated using:
  - `processed_events` table (DB-side idempotency), and
  - Redis dedup set (`processed_event_ids`).

## API

### `POST /events`
Accepts:

```json
{
  "id": "evt-1",
  "user_id": "user-1",
  "amount": 100.50,
  "currency": "EUR",
  "timestamp": "2026-06-08T00:00:00Z"
}
```

Returns `202 Accepted`.

### `GET /users/{user_id}/summary`
Returns total USD amount and transaction count.

### `GET /users/{user_id}/transactions?from=&to=&page=&limit=`
Returns paginated transactions filtered by optional timestamp range.

### `GET /metrics`
Returns in-memory metrics counters.

## Run locally with Docker Compose

### Prerequisites
- Docker + Docker Compose

### Start

```bash
docker compose up --build
```

Services:
- API: `http://localhost:8000`
- PostgreSQL: `localhost:5432`
- Redis: `localhost:6379`

Open API docs:
- `http://localhost:8000/docs`

### Stop

```bash
docker compose down
```

## Run tests

```bash
python3 -m pip install -r requirements.txt
python3 -m pytest -q
```

Included unit tests:
- dedup behavior,
- USD conversion in worker flow.

## One trade-off made

I chose in-memory metrics for simplicity. It is lightweight and good for local/demo usage, but metrics reset on process restart.

## What I would change at 10x load

1. Move to durable metrics/monitoring stack (Prometheus + Grafana).
2. Partition queue workload by user or key to scale workers horizontally.
3. Add DLQ strategy and retry caps for poison messages.
4. Batch DB writes and optimize indexes/connection pooling.
5. Consider managed queue/broker (Kafka/RabbitMQ) for higher sustained throughput.
