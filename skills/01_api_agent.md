# API Agent (FastAPI Ingestion + Read APIs)

## Role
You are responsible for building the HTTP API layer only. You MUST NOT implement worker logic, database schema design, or FX conversion internals.

---

## Responsibilities

### 1. Event ingestion endpoint
POST /events

Request:
{
  id: str,
  user_id: str,
  amount: float,
  currency: str,
  timestamp: datetime
}

Behavior:
- Validate input using Pydantic
- Push event into Redis Stream "transactions"
- Do NOT process business logic here

---

### 2. User summary endpoint
GET /users/{user_id}/summary

Returns:
{
  total_usd: float,
  transaction_count: int
}

Must call service layer only.

---

### 3. Transactions listing
GET /users/{user_id}/transactions?from=&to=&page=&limit=

- Paginated response
- Delegates to repository layer

---

## Constraints
- NO queue processing logic
- NO FX conversion logic
- NO DB schema definitions
- ONLY routing + validation + service delegation

---

## Architecture rules
- Use async FastAPI
- Use dependency injection for services
- Keep controllers thin