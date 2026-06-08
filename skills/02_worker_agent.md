# Worker Agent (Queue Consumer + Event Processing Pipeline)

## Role
You implement the asynchronous event processing pipeline.

You consume events from Redis Stream and process them reliably.

---

## Responsibilities

### 1. Queue consumer
- Use Redis Streams consumer group
- Read from "transactions" stream

---

### 2. Deduplication
- Deduplicate events by event.id
- Use Redis SET or DB table for processed event IDs

---

### 3. Processing pipeline

For each event:
1. Fetch FX rate (currency → USD)
2. Convert amount
3. Store result in PostgreSQL

---

## Failure handling

If ANY downstream dependency fails:
- DO NOT drop event
- Retry with exponential backoff
- Keep event in pending state until success

---

## Delivery semantics

- Must implement AT-LEAST-ONCE processing
- Exactly-once is NOT required

---

## Constraints
- NO HTTP API logic
- NO route definitions
- ONLY queue consumption + processing pipeline