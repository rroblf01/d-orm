# Circuit breaker

When a DB has been down for minutes, aggressive retries exhaust
pools and slam the server when it recovers. The circuit breaker
cuts the loop: after N consecutive failures it opens the circuit
and every call short-circuits with `CircuitOpenError`. After a
cooldown it lets one probe through (HALF_OPEN) and reopens if it
fails.

`dorm.contrib.circuit_breaker` (3.4+).

## API

```python
from dorm.contrib.circuit_breaker import (
    CircuitBreaker, CircuitOpenError,
    circuit_breaker, reset_circuit_breakers, get_state,
)

# Per-name singleton (recommended)
cb = circuit_breaker("default", failure_threshold=5, open_window_s=30.0)

def safe_count() -> int | None:
    try:
        with cb:
            return Author.objects.count()
    except CircuitOpenError:
        return None     # cache, default value, etc.

# Async
async def asafe_count() -> int | None:
    try:
        async with cb.aprotect():
            return await Author.objects.acount()
    except CircuitOpenError:
        return None
```

## States

```
                  failures ≥ threshold
   ┌───────┐  ────────────────────────▶ ┌────────┐
   │CLOSED │                            │  OPEN  │
   └───────┘  ◀──── reset / probe OK ── └────────┘
       ▲                                     │
       │                                     │ open_window_s elapsed
       │           ┌──────────────┐          │
       └───────────│  HALF_OPEN   │ ◀────────┘
       probe ok    └──────────────┘
                          │
                          │ probe fails
                          ▼
                     back to OPEN
```

- **CLOSED**: requests pass. Each failure bumps a counter.
- **OPEN**: every call raises `CircuitOpenError` instantly. After
  `open_window_s`, transitions to HALF_OPEN.
- **HALF_OPEN**: one call passes to probe. Success → CLOSED.
  Failure → OPEN with a fresh timer.

## Configuration

```python
cb = circuit_breaker(
    "alias",
    failure_threshold=5,    # consecutive failures to open
    open_window_s=30.0,     # time in OPEN before HALF_OPEN
)
```

## Inspection

```python
state = get_state("alias")
# {"state": "open", "failures": 7, "opened_at": 12345.6}
```

Useful for Prometheus exporters / dashboards.

## Reset (tests)

```python
reset_circuit_breakers()    # drop every breaker
cb.reset()                  # reset just one
```

## Recipe: FastAPI

```python
from dorm.contrib.circuit_breaker import circuit_breaker, CircuitOpenError

@app.get("/users/{pk}")
async def get_user(pk: int):
    cb = circuit_breaker("default")
    try:
        async with cb.aprotect():
            user = await User.objects.aget(pk=pk)
    except CircuitOpenError:
        return JSONResponse(
            {"detail": "service unavailable"},
            status_code=503,
            headers={"Retry-After": "30"},
        )
    return user
```

## Pitfalls

- **Per-process**: each worker maintains its own breaker. A
  worker in OPEN doesn't prevent another from trying. For
  cross-worker coordination, push the counter to Redis.
- **Premature tuning**: `failure_threshold=1` flips on the first
  flake. Measure your real error rate before tightening.
- **False positives**: if your app legitimately raises
  `OperationalError` (e.g. a `select_for_update` that times out),
  the breaker counts those. Filter by exception class if needed.

## More

- [Helpers](circuit-breaker.md)
- API: `dorm.contrib.circuit_breaker`
