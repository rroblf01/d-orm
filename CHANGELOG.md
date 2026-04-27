# Changelog

All notable changes to djanorm are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Migrations are now atomic per migration file.** A failure in
  operation N now rolls back operations 1..N-1 *and* prevents the
  migration from being recorded as applied — previously a partial
  failure could leave the schema half-applied while the recorder
  thought the migration succeeded, requiring manual cleanup. The same
  atomicity now covers `rollback()` and `migrate_to(...)` reverses.
  - To make this work on SQLite, `atomic()` now issues an explicit
    `BEGIN` at depth 0 (the legacy-transaction-control mode of
    `sqlite3` does not auto-begin before DDL or `SELECT`), and
    `execute_script()` uses `conn.execute()` for single-statement SQL
    (the common DDL case) so it participates in the active transaction
    instead of silently committing via `executescript()`'s built-in
    `COMMIT`.
  - On PostgreSQL, `execute_script()` (sync and async) now honours the
    active `atomic()` / `aatomic()` block by reusing its pinned
    connection. Before, every `execute_script()` checked out a new pool
    connection and committed independently, so DDL escaped the
    surrounding transaction.
- **`prefetch_related()` typos no longer silently fall back to N+1.**
  The sync and async paths previously caught a bare `Exception` when
  resolving the prefetch field name, so `prefetch_related("authrs")`
  (typo) would degrade to running follow-up queries one-per-row with no
  warning. They now narrow the catch to `FieldDoesNotExist`, and the
  reverse-FK fallback raises `FieldDoesNotExist` instead of returning
  silently when no descriptor or registry match is found. Async
  prefetch additionally wraps the per-relation failure in a message
  that names the offending relation.
- **`execute_streaming()` refuses to run inside `atomic()` /
  `aatomic()`.** PostgreSQL named cursors require their own
  transaction; the previous fallback to a non-streaming fetch silently
  materialised the whole result set in memory — exactly what callers
  used streaming to avoid. Now raises `RuntimeError` with a clear
  remediation hint.
- **Async PG pool no longer leaks connections on event-loop switch.**
  When the running event loop changes (e.g. between `asyncio.run()`
  invocations), the previously cached pool was abandoned without being
  closed. The wrapper now schedules `pool.close()` on the old loop via
  `asyncio.run_coroutine_threadsafe()` if it's still alive, so the
  underlying sockets are released promptly.

### Added
- **`Manager.iterator(chunk_size=...)` / `Manager.aiterator(...)` proxies.**
  `Author.objects.iterator()` now works without going through
  `.get_queryset().iterator()`, matching the rest of the manager API
  and Django's surface.
- **`transaction.on_commit(callback)` and `transaction.aon_commit(...)`.**
  Schedule callbacks that fire only after the surrounding transaction
  actually commits — the canonical Django pattern for sending email,
  enqueueing background jobs, or publishing events from inside a
  write block without leaking effects when the transaction rolls back.
  Async variant accepts both regular callables and coroutine
  functions. Outside an `atomic()` block, callbacks fire immediately.
- **`atomic()` / `aatomic()` context managers expose `set_rollback(True)`.**
  Force a rollback without raising — primarily for test fixtures and
  cleanup helpers. Mirrors Django's `transaction.set_rollback`.
- **`QuerySet.select_for_update(skip_locked=, no_wait=, of=)`.** Three
  new flags, all PostgreSQL-only:
  - `skip_locked=True` skips already-locked rows instead of waiting —
    the canonical "task queue" pattern (`SELECT … FOR UPDATE SKIP LOCKED`).
  - `no_wait=True` raises immediately on contention instead of waiting.
  - `of=("authors", …)` limits the lock to specific tables when
    joining. Identifiers are validated.

  SQLite raises `NotImplementedError` if any of these are passed —
  better than silently ignoring them.
- **`QuerySet.bulk_create(ignore_conflicts=, update_conflicts=, …)`.**
  Native upsert support via `ON CONFLICT … DO NOTHING` (when
  `ignore_conflicts=True`) and `ON CONFLICT … DO UPDATE SET …` (when
  `update_conflicts=True`). Works on both PostgreSQL and SQLite ≥ 3.24.
  `update_conflicts=True` requires `unique_fields=` to identify the
  conflict target; `update_fields=` defaults to every non-PK,
  non-unique column. Async counterpart `abulk_create` mirrors the same
  surface.
- **`QuerySet.alias(**kwargs)` (and `Manager.alias`).** Same shape as
  `annotate()` but the named expression is **not** included in the
  `SELECT` list — usable for `filter()` / `exclude()` / `order_by()`
  without paying the per-row hydration cost. Promote to a real
  projection by re-declaring it via `annotate()`.
- **`dorm.pool_stats(alias)` and `health_check(deep=True)`.**
  `pool_stats()` returns live PostgreSQL pool metrics
  (`pool_size`, `pool_available`, `requests_waiting`,
  `requests_num`, `usage_ms`, `connections_ms`, …) for a Prometheus /
  OTel exporter. `health_check(deep=True)` composes the basic
  `SELECT 1` probe with `pool_stats()` so the same endpoint can serve
  both readiness and observability.
- **`dorm.test` module: `transactional_db` / `atransactional_db`
  fixtures and `DormTestCase` mixin.** Wrap each test in an `atomic()`
  block that rolls back at exit, avoiding the
  `DROP TABLE`/`CREATE TABLE` churn between tests. Drops a typical
  suite's runtime by ~3-5×.
- **`dorm dbshell` CLI command.** Drops into the underlying database
  client (`psql` / `sqlite3`) with credentials already wired from
  settings. The PG password is passed via `PGPASSWORD` env var so it
  doesn't show up in `ps`. `--database` selects an alias.
- **`dorm.contrib.softdelete`: `SoftDeleteModel` abstract mixin and
  managers.** Inherit from `SoftDeleteModel` to get a `deleted_at`
  field, three managers (`objects`, `all_objects`, `deleted_objects`)
  and a `delete(hard=False)` / `restore()` / async equivalents API.
  The default `objects` manager hides soft-deleted rows; `all_objects`
  sees them; `deleted_objects` shows only soft-deleted rows.
- **PostgreSQL `LISTEN` / `NOTIFY` async API.**
  `await async_conn.notify(channel, payload)` and
  `async for msg in async_conn.listen(channel)` give you pub/sub on
  the database itself — no Redis required for small fan-out workloads.
  Channel names are validated as SQL identifiers.
- **`dorm.contrib.otel.instrument()` / `uninstrument()`.** Auto-wires
  the `pre_query` / `post_query` signals to OpenTelemetry spans.
  Idempotent — calling twice replaces the previous wiring. Optional
  dependency on `opentelemetry-api`; raises a helpful `ImportError` if
  not installed.
- **Custom Manager instances declared on a model are now properly
  registered.** Previously `objects = MyCustomManager()` left the
  manager's `model` attribute unset, breaking most queryset
  construction. The metaclass now calls `contribute_to_class` for
  every declared manager, and inherits managers from abstract parents
  before falling back to the auto-default — making
  `dorm.contrib.softdelete` (and any user-written equivalent) work
  out of the box.

### Security
- **DEBUG query logs mask values bound to sensitive columns.** Values
  bound to columns whose name suggests a credential — `password`,
  `passwd`, `secret`, `token`, `api_key`, `apikey`, `authorization`,
  `auth_token`, `access_key`, `private_key` — are replaced with
  `"***"` in DEBUG / slow-query log lines. Non-sensitive columns are
  preserved so debugging stays useful. Query observability signals
  (`pre_query` / `post_query`) still receive the raw params; if you
  ship them to external sinks, sanitise there too.

- **`Cast(output_field=...)` validated against an allowlist.** Previously
  `output_field` was spliced directly into `CAST(expr AS {output_field})`,
  so an attacker-controlled string could inject SQL. Only the documented
  base types (`INTEGER`, `TEXT`, `VARCHAR(N)`, `NUMERIC(N, M)`, …) are
  accepted now; anything else raises `ImproperlyConfigured` at queryset
  build time. **Behaviour change:** misspelled type names that happened to
  work before (e.g. `VARCHA`) now fail loudly.
- **`Signal.send()` no longer silently swallows receiver exceptions.**
  Failures are logged on the `dorm.signals` logger at `ERROR` (with
  traceback) so a broken `post_save` hook is observable. Pass
  `Signal(raise_exceptions=True)` to re-raise instead. Built-in signals
  keep the legacy log-and-suppress semantics for compatibility.
- **CLI / settings paths validated as Python dotted paths.**
  `--settings`, the `DORM_SETTINGS` env var and CLI app labels reject
  filesystem-shaped values (`../etc/passwd`, `foo/bar`, etc.) before
  they reach `importlib`. `_find_migrations_dir()` now uses
  `Path.cwd().joinpath(*app.split("."))` instead of string concatenation.
- **`_resolve_column()` narrowed to `FieldDoesNotExist`.** It used to
  catch `Exception`, masking bugs in custom field implementations and
  silently returning a stale column reference. Other errors now bubble
  up; the literal-fallback branch still re-validates the identifier.
- **PRAGMA `journal_mode` selected from a hard-coded SQL mapping.** Even
  if a future change weakened `_validate_journal_mode`, the SQL we
  execute can only ever be one of the six literals in
  `_JOURNAL_MODE_SQL`. Defence-in-depth, not a behaviour change.
- **`RawQuerySet` placeholder-arity check.** Construction fails fast
  when the count of `%s` / `$N` placeholders disagrees with
  `len(params)` — catches the "I built it with f-strings by mistake"
  pattern before SQL ever leaves the process.

### Changed
- **PostgreSQL pool: per-tenant DB / host names logged at DEBUG.** Open
  / close events still emit at INFO so ops keeps boot-time visibility,
  but metadata that could leak tenant identity now requires DEBUG to
  surface.
- **SQLite streaming cursors closed on early break.** `execute_streaming`
  and its async counterpart wrap iteration in `try / finally` so a
  caller that stops iterating early or raises mid-loop no longer leaks
  cursor state.
- **Auto-discovery distinguishes "missing app" from "broken import".**
  `dorm.conf` re-raises `ModuleNotFoundError` when the missing module
  isn't the app itself (i.e. the app's `models.py` has a real
  dependency error), and re-raises any `SyntaxError` it hits — both
  used to be swallowed.

## [2.0.1] - 2026-04-26

### Performance
- **`_to_pyformat()` cached with `functools.lru_cache(4096)`** — the
  ``$N`` → ``%s`` placeholder rewrite is on every PG query's hot path.
  Real apps reuse the same SQL strings billions of times across
  requests; caching converts the per-call O(len(sql)) state-machine
  into a dict lookup.
- **PG ``__in`` lookup uses `= ANY(%s)`** instead of
  ``IN (?, ?, ...)``. One prepared-statement shape regardless of the
  list length, so PostgreSQL's plan cache hits across calls with
  different ``len(ids)``. SQLite stays on the classic ``IN`` syntax.
- **M2M `add()` / `aadd()` batched into 2 queries** — previously
  ``for obj in objs: SELECT 1; INSERT VALUES (..)`` issued ``2N``
  round-trips. Now one ``SELECT ... WHERE tgt IN (...)`` to find
  existing links + one multi-row ``INSERT`` for the missing ones.
  Adding 1000 tags drops from ~2000 queries to 2.
- **M2M `remove()` / `aremove()` batched into 1 query** —
  ``DELETE ... WHERE tgt IN (...)`` instead of N per-object DELETEs.
- **Async `prefetch_related` parallelized via `asyncio.gather`** —
  previously each prefetched relation awaited sequentially, so
  ``.prefetch_related("author", "category", "tags")`` cost 3× the
  latency of one. Now they fire concurrently; total wait collapses
  to the slowest single sub-query.
- **`bulk_create()` / `abulk_create()` field list hoisted out of the
  batch loop** — small constant-factor win (~5%) when the call has
  many batches, and clearer code.

### Added
- **PostgreSQL pool `PREPARE_THRESHOLD` setting** —
  ``DATABASES["default"]["PREPARE_THRESHOLD"]`` is forwarded to
  psycopg's connection ``kwargs``. Set ``0`` for "always prepare"
  on workloads dominated by repeated SELECT/UPDATE shapes; leave
  unset to keep psycopg's default of 5. Both sync and async wrappers
  honour it.

## [2.0.0]

### Security
- **SQLite ``journal_mode`` whitelist** — ``DATABASES["default"]["OPTIONS"]
  ["journal_mode"]`` is now validated against the documented set
  (``DELETE``, ``TRUNCATE``, ``PERSIST``, ``MEMORY``, ``WAL``, ``OFF``)
  before being spliced into ``PRAGMA journal_mode = ...``. Previously
  any string was interpolated verbatim — a misconfigured value such as
  ``"WAL; DROP TABLE dorm_migrations; --"`` would have executed as DDL.
  Defence-in-depth: the value comes from a trusted ``settings.py``, but
  configs populated from environment variables / vault secrets now fail
  fast with ``ImproperlyConfigured`` instead of running arbitrary SQL.

### Fixed
- **Async ``execute_script()`` deadlock inside ``aatomic()``** — the
  async SQLite wrapper held its outer ``_lock`` for the entire
  ``aatomic`` block, but ``execute_script`` tried to re-acquire the
  same (non-reentrant) lock, hanging the coroutine forever. It now
  goes through ``_operation_conn`` and reuses the already-held atomic
  connection. (``execute_script`` is called by user code that runs
  ``RunSQL`` migrations or raw DDL.)
- **Sync ``execute_script()`` redundant commit** — ``sqlite3``'s
  ``executescript()`` already commits implicitly, so the explicit
  ``conn.commit()`` afterwards was a no-op round-trip. Removed; both
  sync and async ``execute_script`` now document SQLite's behaviour
  of committing the surrounding transaction.

### Added
- `dorm` CLI: `init` subcommand to scaffold `settings.py` (and optionally an
  app folder via `--app NAME`); `help` subcommand showing all available
  commands.
- `python -m dorm <command>` is now a valid entry point alongside the `dorm`
  console script.
- QuerySet is awaitable: `rows = await Author.objects.values("name").filter(...)`
  materializes the queryset without needing a terminal `avalues()`/`alist()`.
- Async parity: `abulk_update()` on QuerySet and Manager (mirrors
  `bulk_update`); `_aprefetch_m2m` and `_aprefetch_reverse_fk` so the
  async prefetch path now covers M2M and reverse-FK relations (it
  previously only handled forward FK).
- `atomic` and `aatomic` work as decorators in addition to context managers
  (e.g. `@dorm.transaction.atomic` or `@dorm.transaction.aatomic("alias")`).
- PostgreSQL settings: `POOL_TIMEOUT` (seconds to wait for a free pool
  connection, default 30.0) and `POOL_CHECK` (default `True`; set `False`
  to skip the per-checkout `SELECT 1` health probe on hot paths).
- SQLite settings: `OPTIONS["journal_mode"]` to opt into WAL or other
  journal modes (default keeps SQLite's default DELETE journal).
- SQL logging: `dorm.db.backends.<vendor>` loggers emit each statement at
  `DEBUG` and queries above `DORM_SLOW_QUERY_MS` (default 500ms) at `WARNING`.
- Migration locking: `dorm migrate` acquires an advisory lock on PostgreSQL
  (and a write lock on SQLite) so concurrent invocations across processes
  serialize instead of racing.
- Identifier validation: `Meta.db_table`, `db_column`, M2M `db_table`, and
  `related_name` are validated against a safe-identifier regex at model
  attach time, raising `ImproperlyConfigured` for unsafe names.

### Changed
- **`bulk_update()` rewritten as a single `UPDATE ... SET col = CASE pk WHEN
  ... END` per batch (1 query, not N).** Same change applies to
  `abulk_update()`. The `batch_size` parameter is now actually honored
  (it was previously ignored).
- **M2M prefetch is one JOIN, not two SELECTs.** `prefetch_related("tags")`
  now issues a single query that joins the through table to the target
  table, rather than fetching the through rows and then the targets in a
  second pass.
- `aiosqlite` connection thread is marked daemon before start so the
  Python interpreter can exit even if the user forgets to await
  `connection.close()` (Python 3.13+ joins non-daemon threads before
  `atexit`, otherwise hangs).
- The `dorm` CLI no longer fails silently when no apps are detected; it
  emits a warning to stderr explaining the autodiscovery rules.
- `_load_settings` now puts both the directory containing `settings.py`
  and its parent on `sys.path`, supporting flat layouts (apps next to
  settings) and dotted-package layouts (`myproj/settings.py` with
  `INSTALLED_APPS=["myproj.app"]`) without extra config.
- App import errors during `_load_apps` are surfaced to stderr instead of
  being silently swallowed; an app whose `models.py` has a real import
  problem now produces a clear warning.

### Fixed
- **PostgreSQL `execute_insert` no longer hardcodes `RETURNING id`.**
  Models with a custom PK column name (e.g. `db_column="user_id"`) used
  to fail on PG; the backend now honors `meta.pk.column`.
- `_ado_insert` (async insert) used to include M2M fields in the INSERT
  column list (their `column` is `None`, which produced `INSERT INTO t
  ("title", "None")` and a SQL error). It now skips column-less fields
  and applies field defaults the same way the sync path does.
- `asyncio.get_event_loop()` replaced by `asyncio.get_running_loop()`
  in the async backends (the former is deprecated in Python 3.12+
  and slated for removal in 3.16+).
- Async pool / connection cleanup on event-loop change: when the running
  loop changes between `asyncio.run()` calls, the stale wrapper
  reference is dropped instead of being awaited on the new loop
  (prevents fragile cross-loop cleanup).
- `_to_pyformat` no longer rewrites `$N` occurrences inside SQL string
  literals or quoted identifiers — it now parses tokens correctly,
  so user-supplied data containing `$N` is no longer corrupted.
- The forced `PRAGMA journal_mode = WAL` on SQLite is gone. SQLite's
  default `DELETE` journal mode is now used unless you opt into WAL via
  `DATABASES["default"]["OPTIONS"]["journal_mode"] = "WAL"`. (No more
  surprise `db.sqlite3-shm` / `db.sqlite3-wal` files.)

### Performance
- Single-query `bulk_update`/`abulk_update`: with 1000 rows the round-trip
  count drops from 1000 to 1.
- Single-query M2M prefetch: `prefetch_related("tags")` issues 2 SELECTs
  total (base + JOIN), down from 3 (base + through + targets).
- `POOL_CHECK=False` removes the `SELECT 1` probe from each PG pool
  checkout, saving ~0.1–1 ms per query on hot paths.

### Docs
- README sections expanded for: async cancellation behavior, mixing sync
  and async on SQLite, atomic-as-decorator form, awaiting a queryset,
  `POOL_CHECK` setting, web framework integration (FastAPI / Starlette /
  Flask), batch sizing guidance, and a "Production deployment" section
  covering logging, migration safety, pool sizing, and shutdown.

### Operational tooling
- **`dorm migrate --dry-run`** prints the exact SQL that would run
  without touching the database. Recorder is not updated, so the
  next plain ``migrate`` re-detects the same pending migrations.
  Pre-deploy review gate for SREs / DBAs.
- **`QuerySet.explain(analyze=True)` / `aexplain()`.** Returns the
  database's query plan as a string — ``EXPLAIN (ANALYZE, BUFFERS)``
  on PG, ``EXPLAIN QUERY PLAN`` on SQLite. Diagnose slow production
  queries without leaving Python.
- **`dorm sql <Model>`** (or ``--all``) prints the ``CREATE TABLE`` DDL
  for one or more models. Useful for sharing schema with DBAs or for
  diffing against production by hand.

### New field types
- **``ArrayField(base_field)``** for native PostgreSQL array columns.
  Accepts list / tuple / iterator inputs; ``db_type`` raises
  ``NotImplementedError`` on SQLite so the limitation surfaces at
  migrate time rather than at first query.

### New lookups
- ``array_contains`` (``@>``), ``array_overlap`` (``&&``),
  ``json_has_key`` (``?``), ``json_has_any`` (``?|``),
  ``json_has_all`` (``?&``) — vendor-specific membership / key
  checks for PG arrays and JSONB columns. The pre-existing
  ``__contains`` lookup stays string-LIKE for back-compat; reach
  for the explicit array/json names when the column type demands it.

### Build / CI
- **Coverage gate** in CI: ``--cov-fail-under=73`` so accidental
  drops break the build. Raise the threshold whenever you add tests.
- **Dependabot config** (``.github/dependabot.yml``): weekly grouped
  PRs for pip + GitHub Actions versions.
- **`docs` extra + GitHub Pages workflow** (``mkdocs-material`` +
  ``mkdocstrings``) — `mkdocs serve` for local preview, automatic
  deploy to `gh-pages` on every push to ``main``.

### Docs
- **API reference site** (``docs/index.md``, ``docs/api/*.md``,
  ``mkdocs.yml``) auto-generates from package docstrings.
- **`docs/migration-from-django.md`** — cheat sheet for users
  porting code from Django ORM to dorm.
- **README sections**: Secrets management (env vars / pydantic-settings
  / AWS Secrets Manager), OpenTelemetry integration snippet for the
  query observability hooks.
- **Bilingual documentation site** (English + Spanish) via
  ``mkdocs-static-i18n`` with the ``suffix`` layout (``foo.en.md`` /
  ``foo.es.md``). New full-length guides shipped in both languages:
  Getting started, Tutorial, Models & fields, Querying, Async
  patterns, Migrations, Transactions, FastAPI integration, CLI
  reference, Production deployment, Cookbook, Troubleshooting, and
  Migration from Django ORM. The auto-generated API reference stays
  English-only (docstrings) with Spanish stubs that link back.

### Production deployment helpers
- **Health check.** ``dorm.health_check(alias)`` and
  ``dorm.ahealth_check(alias)`` run ``SELECT 1`` against the configured
  backend and return a JSON-shaped status dict suitable for
  Kubernetes / ALB / Render readiness probes. Never raises — health
  endpoints have to answer the orchestrator even when the DB is down.
- **Pool stats.** ``wrapper.pool_stats()`` returns ``{vendor, open,
  min_size, max_size, pool_size, pool_available, requests_waiting,
  ...}`` for ad-hoc inspection or Prometheus exporters. Sync and async
  PG wrappers expose the full psycopg-pool stats; SQLite returns a
  minimal shim for API parity.
- **PG connection lifecycle settings.** New ``MAX_IDLE`` (default 10 min)
  and ``MAX_LIFETIME`` (default 1 hour) on each ``DATABASES`` entry —
  passes through to psycopg-pool so long-lived workers don't pile up
  stale conns behind PgBouncer / RDS Proxy.
- **Multi-DB / read replicas.** New ``DATABASE_ROUTERS`` setting; each
  router is an object with optional ``db_for_read(model, **hints)`` /
  ``db_for_write(model, **hints)`` methods. ``Manager.get_queryset()``
  consults routers when no explicit ``using=`` is set, so existing
  call sites pick up replica routing with zero changes.
- **Server-side cursors for streaming on PG.** ``iterator(chunk_size=N)``
  / ``aiterator(chunk_size=N)`` now use a server-side named cursor on
  PostgreSQL (so multi-million-row scans don't load the whole result
  set into client memory) and ``cursor.arraysize`` on SQLite. Without
  ``chunk_size``, the previous all-rows-then-iterate path is preserved.
- **Async cancellation safety test.** New regression test exercising
  ``asyncio.wait_for`` mid-query: the pool's ctx-manager returns the
  connection, no leaks even when a coroutine is cancelled.
- **Tutorial doc.** ``docs/tutorial.md`` walks a new user from install
  to a working FastAPI ``/users`` API in 5 minutes — a learning
  on-ramp that the long reference README didn't provide.

### CI
- **PG version matrix.** A second job runs the suite against PostgreSQL
  13 / 14 / 15 / 17 (in addition to 16 in the Python matrix), catching
  version-specific quirks in advisory locks, IDENTITY columns and
  syntax.

### Production hardening
- **Transient-error retry.** PostgreSQL execute paths automatically retry
  ``OperationalError`` / ``InterfaceError`` (network blips, server
  restart, RDS failover) up to ``DORM_RETRY_ATTEMPTS`` (default 3) with
  exponential backoff (``DORM_RETRY_BACKOFF`` seconds, default 0.1s).
  Retries are disabled while inside a transaction so committed work is
  never re-applied. SQLite retries on "database is locked" too. Helpers
  ``with_transient_retry`` / ``awith_transient_retry`` are exposed in
  ``dorm.db.utils`` for user-driven retry of arbitrary code.
- **Query observability hooks.** New ``dorm.pre_query`` and
  ``dorm.post_query`` ``Signal`` instances fire around every SQL
  statement. ``post_query`` receivers also see ``elapsed_ms`` and
  ``error`` (or ``None``), which is enough to wire OpenTelemetry,
  Datadog, Prometheus, or any custom metric / tracing backend without
  patching dorm internals.
- **Lifecycle INFO logs.** Pool open and close events log at INFO on
  ``dorm.db.lifecycle.postgresql`` (db, host, pool sizes, timeout,
  check flag). Per-query DEBUG and slow-query WARNING channels are
  unchanged.

### Pydantic / FastAPI
- **Nested relations in ``DormSchema``.** ``Meta.nested`` now accepts a
  mapping ``{field_name: SubSchema}``: ForeignKey / OneToOne fields
  serialize as the sub-schema (``Type | None`` if nullable),
  ManyToManyField serializes as ``list[SubSchema]``. Lets a FastAPI
  ``response_model`` deliver embedded objects directly, no manual
  validators needed.

### CLI
- **``dorm dbcheck``.** Compares each model's column set with the live
  database schema and prints drift (missing tables, columns missing in
  the DB, columns missing in the model). Exits non-zero when drift is
  found, so it doubles as a pre-deploy gate.

### Versioning
- README adds a *Versioning and deprecation policy* section: SemVer
  scope, deprecation cycle, stable / unstable surfaces.

### Type safety
- **`Field` is now generic in the stored Python type** (`Field[str]`,
  `Field[int]`, `Field[datetime]`, …). Each concrete subclass declares
  its T parameter, so static type checkers (mypy / pyright / ty) see
  ``user.name`` (where ``name = CharField(...)``) as ``str`` rather
  than ``Any``. Same idea SQLAlchemy 2.0 used with ``Mapped[T]``.
  Runtime is unchanged.
- **`ManagerDescriptor` is generic in the model type**, so
  ``Author.objects`` is statically ``BaseManager[Author]`` and the
  whole queryset chain preserves the row type:
  ``Author.objects.filter(...).first()`` is ``Author | None``.
- **`_ForeignKeyIdDescriptor`** — a typed read/write descriptor is
  installed for the underlying ``<fk>_id`` slot when a ForeignKey is
  attached. ``obj.author_id`` is now strictly ``int | None`` instead of
  ``Any``, and writing through it invalidates the FK's cached related
  instance so the next ``obj.author`` re-fetches with the new pk.
  (For full static type-safety on `_id` access, also add a class-level
  ``author_id: int | None`` annotation — runtime descriptors aren't
  visible to type checkers.)

### FastAPI / Pydantic interop
- New module `dorm.contrib.pydantic`:
  - **`DormSchema`** — `BaseModel` subclass with a Django-REST-style
    `class Meta` that auto-fills fields from a dorm Model. ``Meta``
    accepts ``model``, ``fields`` (or ``exclude``), and ``optional``.
    Anything declared in the class body — overrides, extra fields,
    ``@field_validator`` decorators — wins over the Meta-derived
    defaults. ``from_attributes=True`` is set automatically so FastAPI
    can use a dorm instance as a ``response_model`` directly.
  - `schema_for(model_cls, *, name, exclude, only, optional, base)` —
    one-line auto-generation when you don't need a class block. The
    returned class has fields built at runtime, so type checkers see
    it as `type[BaseModel]`. Use `DormSchema` for typing-sensitive code.
  - M2M fields are excluded (no row-level column); FK / O2O serialize
    as the underlying PK column type.
- New optional extra `pydantic` (`pip install 'djanorm[pydantic]'`).
  No `email-validator` dependency — dorm validates the email format
  itself (see below).

### Validation
- **`EmailField` now rejects invalid addresses on construction.**
  Previously the regex check only ran from ``model.full_clean()``,
  which ``save()``/``objects.create()`` do not call — so
  ``Customer.objects.create(email="example")`` happily wrote a row
  with a bogus value. The check moved into ``EmailField.to_python``
  (invoked by ``__set__`` and by ``Model.__init__``), so:

  ```python
  Customer(email="example")              # ValidationError now
  Customer.objects.create(email="example")  # ValidationError now
  customer.email = "example"             # ValidationError now
  ```

  Reads from the database go through ``from_db_value`` (direct dict
  write) and are *not* re-validated, so historical bad rows still
  load.
- **`Model.__init__` no longer swallows ValidationError.** The
  previous ``except Exception`` around field assignment is now
  narrowed to ``except FieldDoesNotExist``, so format errors raised
  by ``to_python`` (EmailField etc.) propagate to the caller instead
  of being silently dropped.

### Build / CI
- `aiosqlite` upper-bound: `<0.23`. The daemon-thread fix relies on a
  private aiosqlite attribute that may move in future versions; bump the
  cap deliberately after re-verifying.
- GitHub Actions `test.yml` now starts a real Postgres service container
  and exposes `DORM_TEST_POSTGRES_*` env vars; conftest prefers that
  service over testcontainers in CI, eliminating the "PG tests silently
  skipped" blind spot. Tests run with `pytest -n 4`; each xdist worker
  gets its own Postgres database to avoid cross-worker collisions.
