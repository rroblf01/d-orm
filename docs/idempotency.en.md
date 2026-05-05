# Idempotency keys

A client submits a payment. Network blip. Client retries. Without
idempotency keys, the payment runs twice.

`dorm.contrib.idempotency` (4.0+) implements the canonical
Stripe pattern: the client sends an `Idempotency-Key: <UUID>`
header per logical operation; on the first request the server
runs the work and stores the response keyed by it; retries return
the cached response instead of re-running.

## API

```python
from dorm.contrib.idempotency import (
    IdempotencyRecord, idempotency_key, purge_expired,
)

# 1. Define your table
class IdpEntry(IdempotencyRecord):
    class Meta:
        db_table = "idempotency_entries"

# 2. Wrap the non-idempotent logic
with idempotency_key(request.headers["Idempotency-Key"], model=IdpEntry) as ctx:
    if ctx.replay:
        return ctx.cached_response, ctx.cached_status_code
    result = process_payment(...)
    ctx.store(result, status_code=201)
    return result, 201

# 3. Cron: purge old rows
purge_expired(IdpEntry, older_than_seconds=86400 * 7)   # 7 days
```

## Atomicity

`idempotency_key()` wraps the block in `atomic()` — the
idempotency row commits **alongside** the business write. If
anything fails, both roll back.

```python
with idempotency_key(key, model=IdpEntry) as ctx:
    Order.objects.create(...)        # business write
    ctx.store({"order_id": order.pk})
    raise SomethingBad()
# → both reverted. The client retries and re-processes.
```

## Race conditions

Two simultaneous requests with the same key:

1. Both do `SELECT` (miss).
2. Both run the logic.
3. The first does `INSERT` → OK.
4. The second does `INSERT` → `IntegrityError` from UNIQUE.

The `atomic()` rollback cleans it up. The client retries and
sees the first request's cached response.

For high concurrency, consider a `select_for_update()` before the
work — serialises every concurrent retry until the first one
finishes. Cost: serialisation per key.

## Payload validation

`ctx.store(response)` validates that `response` is
JSON-serialisable before persisting:

```python
with idempotency_key(key, model=IdpEntry) as ctx:
    ctx.store({"x": some_object})    # ValueError if not JSON
```

Better to fail at write than at cache read hours later.

## Recipe: FastAPI middleware

```python
from fastapi import Header, Request
from fastapi.responses import JSONResponse
from dorm.contrib.idempotency import idempotency_key

@app.post("/payments")
async def create_payment(
    body: PaymentIn,
    idempotency_key_header: str = Header(alias="Idempotency-Key"),
):
    with idempotency_key(idempotency_key_header, model=IdpEntry) as ctx:
        if ctx.replay:
            return JSONResponse(
                ctx.cached_response,
                status_code=ctx.cached_status_code or 200,
            )
        result = await process_payment(body)
        ctx.store(result, status_code=201)
        return JSONResponse(result, status_code=201)
```

## TTL purge

Rows never delete themselves. Cron job:

```bash
# Purge rows >7 days
0 3 * * * cd /app && python -c "
from dorm.contrib.idempotency import purge_expired
from myapp.models import IdpEntry
purge_expired(IdpEntry, older_than_seconds=604800)
"
```

7 days is aggressive if clients retry after hours; 30 days for
retries-after-days. Trade-off: table size vs. protection window.

## Pitfalls

- **Forgetting `ctx.store()` on success**: the block commits
  without saving the response — the next retry re-runs. Stay
  disciplined.
- **Weak keys**: if the client reuses the same key for different
  operations, you return the old response. UUID per operation,
  not per session.
- **Heavy payload validation**: `json.dumps` of a giant dict is
  slow. For huge responses, store only a fingerprint and rebuild
  on demand.

## More

- [Helpers](helpers.md#idempotency-outbox-primitive)
- [Outbox pattern](outbox.md) — comparable, different problem
- API: `dorm.contrib.idempotency`
