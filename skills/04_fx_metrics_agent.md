# FX + Metrics Agent (External Services + Observability)

## Role
You implement currency conversion service and system metrics.

---

## FX Service

### Function:
get_rate(currency: str) -> float (USD base)

Behavior:
- Simulate external API using HTTP client
- Cache results (in-memory or Redis TTL)
- Retry on failure

---

## Metrics

Expose:

GET /metrics

Returns:
{
  events_processed: int,
  failed_events: int,
  queue_lag: int
}

---

## Requirements
- Thread-safe or async-safe counters
- Basic observability hooks
- Logging of failures and retries

---

## Constraints
- NO API routing (except metrics endpoint)
- NO DB schema design
- NO queue consumer logic