# Storage Agent (PostgreSQL + Repository Layer)

## Role
You define database schema and implement all data access logic.

---

## Database schema

### transactions table
- id (primary key, event id)
- user_id (indexed)
- amount_original
- currency
- amount_usd
- timestamp (indexed)

---

### processed_events table (optional dedup support)
- id
- processed_at

---

## Responsibilities

### 1. Data access layer
Implement async repository methods:

- insert_transaction(event)
- get_user_summary(user_id)
- get_user_transactions(user_id, from, to, pagination)

---

### 2. Performance requirements
- Index user_id + timestamp
- Optimize for aggregation queries
- Avoid N+1 queries

---

## Constraints
- NO API logic
- NO worker logic
- ONLY DB + repository layer