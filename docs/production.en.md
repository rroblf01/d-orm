# Production deployment

This page collects everything you should think about when running
dorm beyond a single laptop: connection pooling, replicas, retries,
observability, and the deploy workflow.

## Connection pool sizing

PostgreSQL settings (`DATABASES["default"]`):

| Key | Default | Notes |
|---|---|---|
| `MIN_POOL_SIZE` | `1` | how many idle connections to keep open |
| `MAX_POOL_SIZE` | `10` | hard cap; checkouts beyond this wait |
| `POOL_TIMEOUT` | `30.0` | seconds before a checkout raises `PoolTimeout` |
| `POOL_CHECK` | `True` | run `SELECT 1` on checkout to drop stale conns |
| `PREPARE_THRESHOLD` | psycopg default (5) | after how many executions of the same SQL shape psycopg server-prepares it. Set `0` for "always prepare" on apps dominated by repeated queries; raise it for workloads with many one-shot queries |
| `MAX_IDLE` | `600.0` | recycle conns idle for more than N seconds |
| `MAX_LIFETIME` | `3600.0` | recycle every conn after N seconds, regardless of activity |

**Sizing rule of thumb**: `MAX_POOL_SIZE = vCPU * 2` per process.
Multiply by the number of worker processes (gunicorn, uvicorn) to
get the total connection footprint, and make sure that fits in
PostgreSQL's `max_connections` with room to spare for replication
slots and admin sessions.

```python
DATABASES = {
    "default": {
        "ENGINE": "postgresql",
        "NAME": "myapp",
        "USER": "myapp",
        "PASSWORD": "...",
        "HOST": "primary.internal",
        "PORT": 5432,
        "MIN_POOL_SIZE": 4,
        "MAX_POOL_SIZE": 20,
        "POOL_TIMEOUT": 10.0,
    }
}
```

If you're behind PgBouncer in transaction mode, drop `MIN_POOL_SIZE`
to 1 — the bouncer is the real pool, dorm just needs cheap
checkouts.

## Read replicas

Define every alias in `DATABASES`, then route via `DATABASE_ROUTERS`:

```python
DATABASES = {
    "default": {"ENGINE": "postgresql", "HOST": "primary.internal", ...},
    "replica": {"ENGINE": "postgresql", "HOST": "replica.internal", ...},
}

class PrimaryReplicaRouter:
    def db_for_read(self, model, **hints):
        return "replica"
    def db_for_write(self, model, **hints):
        return "default"

DATABASE_ROUTERS = [PrimaryReplicaRouter()]
```

Routers can also branch on the model:

```python
class AuditRouter:
    def db_for_write(self, model, **hints):
        if model._meta.app_label == "audit":
            return "audit_writer"
        return None      # let other routers / default decide
```

For a one-off override, use `Manager.using("alias")` — it bypasses
the routers for that single query.

## Transient retry

dorm retries `OperationalError` and `InterfaceError` (network blips,
server restarts) on both sync and async pools. Tunable via env vars:

| Var | Default | Effect |
|---|---|---|
| `DORM_RETRY_ATTEMPTS` | `3` | total attempts including the first one |
| `DORM_RETRY_BACKOFF` | `0.1` | seconds, multiplied by `2^attempt` |

Retries are **disabled inside transactions** — the pool can't safely
replay a half-committed `BEGIN`. Wrap external "must-succeed"
sequences in your own retry loop with idempotency keys instead.

## Health checks

```python
import dorm

@app.get("/healthz")
async def healthz():
    return await dorm.ahealth_check()
```

`health_check()` (sync) and `ahealth_check()` (async) execute
`SELECT 1` on the configured alias and return:

```python
{"status": "ok", "alias": "default", "elapsed_ms": 0.42}
{"status": "error", "alias": "default", "elapsed_ms": 5012.0,
 "error": "OperationalError: connection refused"}
```

Both never raise — they always respond, even when the DB is down,
which is what an orchestrator readiness probe needs.

## Migration deploys

The recommended deploy order:

1. Build the new code (immutable artifact).
2. `dorm migrate --dry-run` against production — review the SQL.
3. `dorm migrate` (advisory locks make concurrent runs safe).
4. Roll out the new code.

For zero-downtime schema changes, follow the standard expand/contract
playbook:

| Step | Migration | Code |
|---|---|---|
| Expand | add column nullable | old code ignores it |
| Backfill | data migration in chunks | old + new run side-by-side |
| Contract | mark column NOT NULL, drop old | new code only |

`dorm dbcheck` in your CI pipeline catches the case where a developer
forgot to commit a migration: it exits non-zero on schema drift.

## Observability

### Per-query hooks

```python
from dorm.signals import pre_query, post_query

def trace(sender, sql, params, alias, duration_ms=None, **kwargs):
    log.info("query", sql=sql, params=params, alias=alias, ms=duration_ms)

pre_query.connect(trace)
post_query.connect(trace)
```

Connect these to OpenTelemetry, structlog, or whatever you use. The
`post_query` signal includes `duration_ms`, which is what you want
to feed your APM.

### Pool stats

```python
from dorm.db.connection import get_connection
stats = get_connection("default").pool_stats()
# {"size": 7, "idle": 4, "in_use": 3, "max_size": 20, ...}
```

Expose this on `/metrics` via Prometheus to graph saturation. A pool
that hits `in_use == max_size` for sustained periods is the leading
indicator of a connection-bound app.

### EXPLAIN

For one-off debugging, `qs.explain(analyze=True)` returns the planner
output. Wire it into a dev-only endpoint or use it in `dorm shell`.

## Async event-loop sharing

If you run async code (FastAPI, asyncio scripts), make sure all your
tests share **one** event loop:

```toml
# pyproject.toml
[tool.pytest.ini_options]
asyncio_mode = "auto"
asyncio_default_fixture_loop_scope = "session"
asyncio_default_test_loop_scope = "session"
```

Without this, every test creates a fresh loop and a fresh pool. Old
pools linger as dangling connections, max_connections climbs, and
eventually CI starts hanging.

## Logging

dorm uses the stdlib `logging` module under the `dorm` namespace.
Useful loggers:

| Logger | What it emits |
|---|---|
| `dorm.db.pool` | INFO on pool open/close, WARNING on exhaustion |
| `dorm.db.lifecycle.postgresql` | INFO on PG pool open/close (size/timeout); DB name + host at DEBUG only, so per-tenant metadata never reaches an INFO sink unless you explicitly enable it |
| `dorm.migrations` | INFO per applied migration |
| `dorm.queries` | DEBUG per executed SQL (off by default) |
| `dorm.signals` | ERROR per receiver exception (with full traceback) — wire this to Sentry / your alert pipeline so a broken `post_save` hook is observable |
| `dorm.conf` | INFO when a `settings.py` is autodiscovered (audit trail for which file shaped the config) |

```python
import logging
logging.getLogger("dorm.queries").setLevel(logging.DEBUG)
# Route signal failures to your alerting handler:
logging.getLogger("dorm.signals").addHandler(your_alert_handler)
```

## Checklist

- [ ] `MAX_POOL_SIZE × workers ≤ Postgres max_connections / 2`
- [ ] `dorm dbcheck` runs in CI
- [ ] `dorm migrate --dry-run` runs as a deploy gate on prod
- [ ] `/healthz` wired to readiness probe
- [ ] `pre_query` / `post_query` traced into your APM
- [ ] Async tests use a session-scoped event loop
- [ ] Replica router defined if traffic > 1 box
