# Upgrading from 4.1 to 4.2

No code changes required. Every v4.2 addition is opt-in or zero-cost
when unused.

## Compatibility

- Python 3.11+ (unchanged).
- ``djanorm-mypy`` / ``pytest-djanorm`` sibling packages continue to
  work without bumping (their floor pins are ``djanorm >= 4.0``).
- Existing migrations re-apply unchanged — every new
  :class:`Operation` (``MakeTableAppendOnly``, ``AlterColumnTypeOnline``)
  ships as a new class; nothing was renamed or removed.

## Behaviour changes worth knowing

These are not API breaks — same call, same return type — but the
defaults trace a different code path than 4.1:

- **`dorm migrate --dry-run <target>`** previously errored with
  exit 1; in 4.2 it prints the rollback SQL. Update CI gates that
  asserted the previous failure.
- **Prometheus metrics output** now carries
  ``dorm_pool_saturation{alias}`` and (when
  :func:`querystats.collector().enable()` has been called)
  ``dorm_template_*`` lines. Dashboards that grep for absent metrics
  will see new keys.
- **`dorm.configure(DEBUG_NPLUSONE=True)`** auto-installs the
  global N+1 detector. If you previously called
  :func:`install_debug_global` explicitly, the second call is a
  no-op — leave it in for backward compatibility.

## New optional dependencies

None. v4.2 ships with the same install matrix as v4.1
(`[postgresql]`, `[litestar]`, `[parquet]`, …).

## Recommended adoption order

The features ship independently — pick what you need. A typical
rollout for a production stack:

1. **`dorm doctor`** — run against the existing config and triage
   the new warnings before adopting any other 4.2 feature.
2. **`dorm.contrib.querystats`** — flip the collector on, watch the
   shape of your query population for a week before deciding what
   needs an index.
3. **Pool saturation gauge** — picks up automatically once
   ``prometheus.metrics_response()`` is called.
4. **`SLOW_QUERY_EXPLAIN=True`** — enable in staging first; the
   doubled cost on slow queries is rarely an issue but watch the
   logs.
5. **`MakeTableAppendOnly`** on `<Model>_history` tables that don't
   need updates.
6. **`sql_allowlist`** — canary capture, curate, enforce.
7. **PG advisory locks** wherever you're hand-rolling a leader
   election or singleton cron.
8. **`Broadcaster`** + **`DataLoader`** in async stacks (GraphQL,
   FastAPI fan-out resolvers).

## Removed / deprecated

Nothing removed. Nothing deprecated.

---

See [What's new in 4.2](v4_2.md) for the full feature catalogue.
