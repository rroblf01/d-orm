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

Pass `deep=True` to also include live pool statistics, so the same
endpoint can serve readiness *and* observability:

```python
await dorm.ahealth_check(deep=True)
# {
#   "status": "ok", "alias": "default", "elapsed_ms": 0.42,
#   "pool": {
#     "alias": "default", "vendor": "postgresql", "has_pool": True,
#     "pool_min": 1, "pool_max": 10,
#     "pool_size": 7, "pool_available": 4, "requests_waiting": 0,
#     "requests_num": 18234, "usage_ms": 412.3, "connections_ms": 1.1,
#     ...
#   }
# }
```

Or call `dorm.pool_stats(alias)` directly if you only want the pool
view (e.g. on a Prometheus exporter):

```python
from dorm.db.connection import pool_stats
stats = pool_stats("default")
```

A pool whose `pool_available` stays at zero with `requests_waiting`
above zero for sustained periods is the leading indicator of a
connection-bound app.

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

### Slow-query warning (`SLOW_QUERY_MS`)

Every executed statement is timed regardless of configuration —
the `pre_query` / `post_query` signals already need the elapsed
time, so no extra cost is paid for the slow-query warning. When the
elapsed time crosses the configured threshold, the
`dorm.db.backends.<vendor>` logger emits a `WARNING` line with the
SQL text:

```
WARNING dorm.db.backends.postgresql: slow query (812.43ms ≥ 500ms): SELECT ...
```

Configuration (first non-`None` source wins):

| Source | Example | Notes |
|---|---|---|
| `settings.SLOW_QUERY_MS` (2.6+) | `dorm.configure(SLOW_QUERY_MS=200)` | always wins; recommended for production |
| Env var `DORM_SLOW_QUERY_MS` | `export DORM_SLOW_QUERY_MS=300` | fallback when no explicit setting |
| Default | `500.0` | applied if neither is configured |

Setting `SLOW_QUERY_MS=None` disables the warning entirely (the
threshold comparison itself is skipped). Setting `0` makes every
query log as slow — useful in development to surface every SQL
statement at WARNING level without flipping the whole DEBUG log
stream on.

The threshold is memoised once after `configure(...)` so the hot
path doesn't redo the lookup per query. A subsequent
`configure(SLOW_QUERY_MS=...)` invalidates the memoised value.

```python
import dorm, logging

# Production: warn on anything slower than 200 ms.
dorm.configure(SLOW_QUERY_MS=200, DATABASES={...})

# Pipe the warning to your alerting handler.
logging.getLogger("dorm.db").addHandler(your_alert_handler)
```

To suppress per-vendor: silence the lower-level logger
(`dorm.db.backends.postgresql`) instead — the whole `dorm.db`
namespace is hierarchical.

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
| `dorm.db.backends.<vendor>` | WARNING per query slower than `SLOW_QUERY_MS` (see [Slow-query warning](#slow-query-warning-slow_query_ms)) |
| `dorm.signals` | ERROR per receiver exception (with full traceback) — wire this to Sentry / your alert pipeline so a broken `post_save` hook is observable |
| `dorm.conf` | INFO when a `settings.py` is autodiscovered (audit trail for which file shaped the config) |

```python
import logging
logging.getLogger("dorm.queries").setLevel(logging.DEBUG)
# Route signal failures to your alerting handler:
logging.getLogger("dorm.signals").addHandler(your_alert_handler)
```

## Security notes

A few sharp edges worth keeping in mind for production deployments:

- **`settings.py` auto-discovery executes Python code.** When you don't
  pass `--settings=` or `DORM_SETTINGS=`, dorm walks `cwd` and the
  directory of `sys.argv[0]` looking for a `settings.py` and
  `exec_module()`s the first match. That's by design (it mirrors
  Django's manage.py behaviour) but it does mean a `settings.py` that
  ends up in your working directory will run as code. **Pass an
  explicit `--settings=myproj.settings` in production runners** so
  there's no ambiguity, and audit your container images for stray
  `settings.py` files.
- **DEBUG-level query logs mask values bound to columns whose name
  matches `password`, `token`, `api_key`, `secret`, …** — the rest is
  printed verbatim to help debugging. If you forward DEBUG logs to a
  shared sink (Datadog, Loki), make sure the redaction list covers
  your domain-specific credential columns; if not, extend it via the
  `dorm.db.utils._SENSITIVE_COLUMN_PATTERNS` tuple, or filter on the
  log handler. The `pre_query` / `post_query` signals always receive
  the raw params; if you fan them out to external sinks, sanitise
  there too.
- **Migrations are atomic per file.** A failure in op N rolls back ops
  1..N-1 and the migration is *not* recorded as applied — so a
  retried `dorm migrate` will reapply cleanly. The same atomicity
  covers rollback / `migrate_to`. On SQLite this required forcing an
  explicit `BEGIN` (Python's `sqlite3` module does not auto-begin
  before DDL); on PostgreSQL all DDL now goes through the connection
  pinned by the active `atomic()` block.
- **`execute_streaming()` refuses inside `atomic()`.** Server-side
  cursors need their own transaction; the previous silent fallback
  loaded the whole result set in memory. If you must stream from
  inside a transaction, restructure the workload (e.g. read PKs into a
  list outside the block, then iterate them).

## File storage

`FileField` writes through `settings.STORAGES`. In production:

- **Don't hardcode credentials.** boto3 reads creds from the standard
  chain — IAM role on EC2/ECS/Lambda, env vars
  (`AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`),
  `~/.aws/credentials`. Leave `access_key` / `secret_key` unset in
  `STORAGES` so the runtime picks the right ones; reserve those
  options for local dev and MinIO.
- **Least-privilege IAM.** The role needs `s3:PutObject`,
  `s3:GetObject`, `s3:DeleteObject`, and `s3:ListBucket` (only for
  `get_available_name`'s collision check) on the configured bucket.
  No bucket-policy ACLs needed if you keep `default_acl="private"` and
  rely on presigned URLs.
- **Presigned URL expiry.** The default `querystring_expire=3600`
  is one hour — long enough for the user to click through, short
  enough that an exfiltrated link goes stale before it's useful.
  Don't crank it to days.
- **Public-CDN delivery.** For static assets behind CloudFront /
  Cloudflare, set `custom_domain` to the CDN hostname. The URL
  `S3Storage.url(name)` returns will skip the signing dance and
  point straight at the CDN, which then fetches from the bucket via
  Origin Access Identity. Saves the per-request signing CPU and
  works with browser caching.
- **Bucket-side lifecycle.** Configure S3 lifecycle rules to
  transition cold uploads to Glacier or expire them after N days.
  dorm doesn't track upload age; expiry is the bucket's job.
- **`FileSystemStorage` backups.** If you keep using local disk in
  production (single-machine apps, on-prem deploys), back the
  `location` directory up alongside the database — the row in
  `documents` and the bytes in `/var/app/media/...` are the same
  logical record split across two stores. Restore both or neither.
- **Path-traversal guard.** `FileSystemStorage._resolve_path` rejects
  any name that would escape `location` — defence-in-depth against
  user-controlled `upload_to` callables. The S3 backend has no
  filesystem to escape, but its `get_valid_name` still strips path
  components.
- **Don't put `MEDIA_ROOT` inside the codebase.** Set `location` to
  a path on a separately-mounted volume so a redeploy doesn't wipe
  user uploads.

For the FastAPI side of file uploads (multipart handling, streaming
downloads, presigned-URL responses), see
[FastAPI: File uploads](fastapi.md#file-uploads).

## Checklist

- [ ] `MAX_POOL_SIZE × workers ≤ Postgres max_connections / 2`
- [ ] `dorm dbcheck` runs in CI
- [ ] `dorm migrate --dry-run` runs as a deploy gate on prod
- [ ] Explicit `--settings=` or `DORM_SETTINGS=` in production runners
- [ ] `/healthz` wired to readiness probe
- [ ] `pre_query` / `post_query` traced into your APM
- [ ] Async tests use a session-scoped event loop
- [ ] Replica router defined if traffic > 1 box
- [ ] `STORAGES` uses IAM role / env-var creds (no hardcoded keys)
- [ ] `MEDIA_ROOT` on a persistent volume (if FileSystemStorage)
- [ ] Presigned-URL expiry tuned for your audit / compliance needs

## URL/DSN configuration (2.1+)

`DATABASES` entries now accept either a URL string or a dict with a
`URL` key. Use this to pull the connection string straight from
`DATABASE_URL` without writing ``HOST/PORT/USER/PASSWORD`` plumbing::

    import os, dorm

    dorm.configure(DATABASES={
        "default": os.environ["DATABASE_URL"],
        # Or, with overrides:
        # "default": {"URL": os.environ["DATABASE_URL"], "MAX_POOL_SIZE": 30},
    })

Recognised query-string knobs (`MAX_POOL_SIZE`, `POOL_TIMEOUT`,
`POOL_CHECK`, `MAX_IDLE`, `MAX_LIFETIME`, `PREPARE_THRESHOLD`) are
lifted to top-level `DATABASES` keys. Anything else lands in
`OPTIONS`.

## Pre-deploy gate: `dorm doctor` (2.1+)

Run `dorm doctor` in CI to fail builds whose configuration trips a
known production footgun. Examples it catches: small pool size,
missing `sslmode` on a remote PG host, FKs without an index,
transient-error retry disabled.
