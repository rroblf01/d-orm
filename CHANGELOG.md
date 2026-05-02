# Changelog

All notable changes to djanorm are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Roadmap (not yet shipped)

- **MySQL / MariaDB backend** (slated v3.1). Mirrors the
  ``postgresql`` backend shape with vendor-specific dialect
  (``ON DUPLICATE KEY UPDATE``, backtick identifiers, no
  transactional DDL). Scope: ~600 LOC + testcontainers fixture.
- **Multi-tenant schema-per-tenant** (slated v3.1). PG
  ``search_path`` switching via a ``TenantContext`` context
  manager + per-tenant migration runner.
- **History / audit trail** (slated v3.1). ``HistoricalModel``
  mixin auto-creates a ``<Model>History`` shadow table fed by
  ``pre_save`` / ``pre_delete``.
- **Connection-pool autoscaling**. Adjust ``MAX_POOL_SIZE`` based
  on observed saturation. Risky — needs careful rollout.
- **`dorm migrate-from-django` converter**. Parses Django
  ``models.py``, emits dorm-shaped equivalents.

## [3.0.0] - 2026-05-02

Major release. Three originally-planned minors (2.6 / 2.7 / 2.8)
land together — the breadth of additions warrants the version
jump but **NO breaking changes versus 2.5.0**. Every new feature
is opt-in or zero-cost when unused; existing apps upgrade with
no behaviour change.

The release groups into three themes:

1. **Observability + dev tooling** (was 2.6) — slow-query
   warning, retry knobs, query-count guard, sticky
   read-after-write window, `dorm lint-migrations`,
   request-scoped `QueryLog`, `LocMemCache`,
   `Manager.cache_get` row cache.
2. **Auth + parity + DX** (was 2.7) — `dorm.contrib.auth`,
   `dorm.contrib.asyncguard`, expanded DB function corpus,
   `Meta.managed = False`, async-aware `dorm shell`,
   benchmark suite skeleton, "no-asgiref" migration docs.
3. **Encryption + metrics** (was 2.8) — `EncryptedCharField` /
   `EncryptedTextField` (AES-GCM, key rotation),
   `dorm.contrib.prometheus` exporter.

### Added — `settings.SLOW_QUERY_MS`

- New setting (default ``500.0`` ms). When an executed statement
  takes longer than this threshold, the
  ``dorm.db.backends.<vendor>`` logger emits a ``WARNING`` line
  with the SQL text and elapsed time. Resolution order: explicit
  ``configure(SLOW_QUERY_MS=…)`` > env var
  ``DORM_SLOW_QUERY_MS`` > default ``500.0``.
- ``SLOW_QUERY_MS=None`` disables the warning entirely — the
  threshold comparison itself is skipped, so on-path cost
  collapses to the timing already collected for the query
  signals.
- ``SLOW_QUERY_MS=0`` logs every query as slow — useful in
  development to surface every SQL statement at WARNING level
  without flipping the ``dorm.queries`` DEBUG stream on.
- The threshold is memoised once after ``configure(...)``;
  subsequent ``configure(SLOW_QUERY_MS=…)`` calls invalidate
  the memo so a runtime swap takes effect on the next query.
  The env-var / default branch is intentionally not memoised so
  test ``monkeypatch.setenv`` workflows keep observing the
  current value without an explicit cache flush.

### Added — `settings.RETRY_ATTEMPTS` / `settings.RETRY_BACKOFF`

- Same resolver shape as ``SLOW_QUERY_MS``: explicit setting >
  env var (``DORM_RETRY_ATTEMPTS`` / ``DORM_RETRY_BACKOFF``) >
  default (3 attempts / 0.1 s). The retry loop in
  ``with_transient_retry`` / ``awith_transient_retry`` now
  reads the resolved value on every call so a runtime
  ``configure(...)`` swap takes effect without a restart.
- Settings-derived values are memoised; env-var path re-reads
  each call so test ``monkeypatch.setenv`` keeps working.

### Added — query-count guard (`dorm.contrib.querycount`)

- New ``query_count_guard(warn_above=…, label=…)`` context
  manager. Counts queries inside the block via ``pre_query``
  and emits a single ``WARNING`` when the count crosses the
  threshold. ``warn_above`` falls back to
  ``settings.QUERY_COUNT_WARN`` (default ``None``, inert).
- Per-task isolation via ``ContextVar`` — concurrent ASGI
  requests / asyncio tasks get independent counters.

### Added — `dorm.test.assertNumQueries` / `assertMaxQueries`

- Django-parity helpers. Context-manager forms assert exact
  / ≤-N query counts on exit; decorator forms
  (``assertNumQueriesFactory(N)``, ``assertMaxQueriesFactory(N)``)
  wrap a sync or ``async def`` test function. Both use the same
  ``ContextVar``-based listener as the query-count guard so
  async tests don't bleed counters across tasks.

### Added — sticky read-after-write window

- ``settings.READ_AFTER_WRITE_WINDOW`` (default ``3.0``
  seconds). After a write through ``router_db_for_write``, the
  router pins reads of the same model on the same context to
  the primary alias for the configured window — so a request
  that writes and immediately re-reads sees its own change
  instead of a stale replica row. ``0`` / ``None`` disables.
- Sticky state lives in a ``ContextVar``, so concurrent ASGI
  requests / asyncio tasks see independent windows.
- New helper ``dorm.db.connection.clear_read_after_write_window``
  for tests / middleware that want a clean window per request.
- Pass ``sticky=False`` through the router hints (e.g.
  ``Model.objects.using("replica", sticky=False)``) to opt out
  of the pin for analytics queries that explicitly want the
  replica.

### Added — `dorm lint-migrations`

- New CLI command + ``dorm.migrations.lint`` API. Walks every
  migration in ``INSTALLED_APPS`` and emits findings for known
  online-deploy footguns. Exits non-zero on findings — wire as
  a CI gate.
- Rules: ``DORM-M001`` AddField NOT NULL with default
  (full-table backfill), ``DORM-M002`` AlterField (review type
  vs. flag-flip), ``DORM-M003`` AddIndex without
  ``concurrently=True`` (PG ACCESS EXCLUSIVE lock),
  ``DORM-M004`` RunPython without ``reverse_code``
  (irreversible).
- ``--format=json`` for machine-parseable output, ``--rule
  DORM-M00X`` to filter, ``--exit-zero`` for advisory CI runs.
  Suppress per-file with ``# noqa: DORM-M00X``.

### Added — `dorm migrate --plan`

- Alias for the existing ``--dry-run`` flag, mirroring
  Django's command name. Prints the SQL that would be
  executed without touching the database. The migration
  recorder is NOT updated.

### Added — request-scoped query collector (`dorm.contrib.querylog`)

- ``QueryLog`` context manager captures every SQL statement
  inside a block: ``sql`` / ``params`` / ``alias`` /
  ``elapsed_ms`` / ``error``. ``summary()`` groups by SQL
  template (placeholders normalised to ``?``) and returns
  ``list[TemplateStats]`` with count / total / p50 / p95
  timings.
- ``QueryLogASGIMiddleware`` wraps any ASGI app; the per-request
  log lands on ``scope["dorm_querylog"]`` for downstream
  handlers / middlewares to inspect.

### Added — `LocMemCache` cache backend (`dorm.cache.locmem`)

- Thread-safe ``OrderedDict``-backed LRU. Same
  :class:`BaseCache` contract as ``RedisCache`` — sync + async
  helpers, ``delete_pattern`` for signal-driven invalidation,
  ``OPTIONS["maxsize"]`` cap.
- Useful for tests, single-process scripts, or as a layer in
  front of Redis. NOT shared across worker processes.

### Added — single-row cache (`Manager.cache_get`)

- ``Model.objects.cache_get(pk=…, timeout=…, using=…)`` reads
  through the configured cache before falling through to the
  DB. Uses the per-model invalidation version so a
  ``post_save`` on the model bumps both queryset-cache entries
  and row-cache entries in lock-step.
- ``cache_get_many(pks=[…])`` — batch read; misses collapse
  into one ``WHERE pk IN (...)`` query and write back to
  cache.
- Async parity via ``acache_get`` / ``acache_get_many``. Cache
  miss / outage falls through silently — same ``try / except``
  policy as ``RedisCache``.

### Added — `dorm.contrib.auth`

- ``User`` / ``Group`` / ``Permission`` models — framework-agnostic.
  Provides only the data model and password-hashing helpers; login
  views, sessions and middleware are NOT included (that's the web
  framework's job).
- Password hashing via stdlib ``hashlib.pbkdf2_hmac`` with format
  ``pbkdf2_sha256$<iterations>$<salt>$<hash>`` — same shape Django
  emits, so passwords migrate cleanly between the two ORMs. No
  ``passlib`` / ``bcrypt`` / ``argon2`` dependency.
- ``UserManager.create_user`` / ``create_superuser`` ensure
  passwords land hashed instead of stored in the clear.
- ``user.set_password()`` / ``check_password()`` / ``has_perm()``
  with both sync and async (``ahas_perm``) variants. Group-based
  permission checks walk forward M2M only — no reverse-M2M
  traversal that would break across vendors.

### Added — `dorm.contrib.asyncguard`

- ``enable_async_guard(mode="warn"|"raise"|"raise_first")`` connects
  to ``pre_query`` and walks the call stack — sync ORM calls
  inside a running event loop trigger the configured action; async
  ORM calls stay silent. Frame walking distinguishes the two paths
  by looking for ``async def`` frames inside the ``dorm`` package.
- ``SyncCallInAsyncContext`` inherits from :class:`BaseException`
  so it bypasses the dispatcher's ``except Exception`` and
  surfaces as a 500, not a logged-and-swallowed warning.
- Recommended for development / test environments — disabled by
  default in production.

### Added — DB function corpus

- Math: ``Power``, ``Sqrt``, ``Mod``, ``Sign``, ``Ceil``, ``Floor``,
  ``Log``, ``Ln``, ``Exp``, ``Random``.
- Util: ``NullIf``.
- String: ``Trim``, ``LTrim``, ``RTrim``.
- All cross-backend on PG and SQLite ≥3.35 (Python 3.11+ ships
  recent enough libsqlite3 by default). MySQL coverage lands with
  the v3.1 backend.

### Added — `Meta.managed = False`

- Models with ``managed = False`` are skipped by
  ``ProjectState.from_apps`` — ``makemigrations`` no longer emits
  a ``CreateModel`` for tables the user marks as externally
  managed (legacy schema, view, foreign data wrapper).
- Runtime queries / saves keep working unchanged — only the
  migration emission path is affected.

### Added — async-aware `dorm shell`

- The fallback REPL (when IPython is absent) now compiles input
  with ``PyCF_ALLOW_TOP_LEVEL_AWAIT`` so
  ``await Article.objects.aget(pk=1)`` works directly. ``--no-async``
  reverts to the classic stdlib REPL.

### Added — benchmark suite skeleton

- ``bench/run.py`` — stdlib-only microbenchmark runner.
  Scenarios: ``create``, ``bulk_create``, ``get``, ``filter_count``,
  ``list_first_n``. JSON output suitable for committing under
  ``bench/results/`` and charting later.
- ``tests/test_v2_7_bench_smoke.py`` — ensures the runner survives
  one end-to-end sqlite run with the smallest parameters.

### Added — `dorm.contrib.encrypted`

- ``EncryptedCharField`` and ``EncryptedTextField`` store
  ciphertext on disk and decrypt transparently on read. Backed by
  AES-GCM via the optional ``cryptography`` package
  (``djanorm[encrypted]``).
- Deterministic mode (default) keeps equality lookup working —
  same plaintext produces the same ciphertext via an
  HMAC-derived nonce. Switch to ``deterministic=False`` for
  random-nonce indistinguishability when equality lookups aren't
  required.
- Key rotation: ``settings.FIELD_ENCRYPTION_KEYS`` accepts a list
  of keys (newest first); decryption tries each in order so old
  rows keep decrypting after a primary-key roll.
- Tampered ciphertext is rejected with a clear ``ValueError``
  rather than silently returning ``None`` — better to surface the
  bug than mask it.
- New settings: ``FIELD_ENCRYPTION_KEY`` (single-key form) and
  ``FIELD_ENCRYPTION_KEYS`` (list-form for rotation).

### Added — `dorm.contrib.prometheus`

- Stdlib-only Prometheus text-exposition exporter — no
  ``prometheus_client`` dependency. Connects to ``post_query`` to
  emit ``dorm_queries_total`` (counter), ``dorm_query_duration_seconds``
  (histogram), plus optional pool / cache gauges.
- ``install()`` attaches the listener; ``uninstall()`` removes it
  and resets state. Idempotent so a FastAPI lifespan can wire it
  without ceremony.
- ``record_cache_hit(alias)`` / ``record_cache_miss(alias)``
  helpers for custom cache backends that want their numbers in the
  exposition.

### Changed — `Settings` tracks explicit overrides

- New private attribute ``Settings._explicit_settings`` records
  the names of settings the user passed to ``configure(...)``.
  Resolvers can now distinguish a class-level default (apply
  env-var or built-in fallback first) from an explicit user
  choice (always wins). Used by every memoised setting (slow
  query, retry knobs, …) — future resolvers register one
  ``MemoizedSetting`` instance and the central registry handles
  the rest.

### CLI

- ``dorm init`` template now scaffolds (commented-out by default
  except for ``SLOW_QUERY_MS``) every new 3.0 knob:
  ``RETRY_ATTEMPTS``, ``RETRY_BACKOFF``, ``QUERY_COUNT_WARN``,
  ``READ_AFTER_WRITE_WINDOW``, ``DATABASE_ROUTERS`` example,
  a ``CACHES`` block with both Redis and LocMemCache examples,
  and ``FIELD_ENCRYPTION_KEY`` placeholder.
- ``dorm lint-migrations`` registered as a top-level subcommand
  with ``--format`` / ``--rule`` / ``--exit-zero`` / ``--settings``
  flags.
- ``dorm migrate --plan`` accepted as an alias for ``--dry-run``.

### Documentation

- ``docs/production.{en,es}.md`` — new subsections under
  *Observability* covering ``SLOW_QUERY_MS``,
  ``RETRY_ATTEMPTS`` / ``RETRY_BACKOFF``, ``QUERY_COUNT_WARN``,
  ``READ_AFTER_WRITE_WINDOW`` and the request-scoped
  ``QueryLog`` collector. New top-level *Migration safety*
  section documenting ``dorm lint-migrations`` rules.
- ``docs/cli.{en,es}.md`` — new ``dorm lint-migrations`` section
  with ``--rule`` / ``--exit-zero`` / ``--format`` flags.
  ``dorm migrate`` flag table now lists ``--plan`` as the
  Django-style alias for ``--dry-run``.
- ``docs/cache_redis.{en,es}.md`` — new ``LocMemCache``
  configuration block and ``Manager.cache_get(pk=…)`` /
  ``cache_get_many(pks=[…])`` row-cache subsection (sync + async).
- ``docs/cookbook.{en,es}.md`` — testing fixtures section grew
  an ``assertNumQueries`` / ``assertMaxQueries`` entry covering
  the context-manager and decorator forms (sync + async).
- ``docs/migration-from-django.{en,es}.md`` — new
  *You don't need ``asgiref``* section with a Django↔dorm
  cheatsheet (every ``sync_to_async(qs.X)`` has a native ``aX``
  counterpart) plus call-outs for the optional ``contrib.auth``,
  ``contrib.encrypted`` and ``contrib.prometheus`` modules.

### Internal

- ``dorm._memoized_setting.MemoizedSetting`` — central registry
  for the ``settings → env → default`` resolver pattern. New
  per-call knobs register one instance and ``conf.configure``'s
  invalidation pulse fans out automatically (no per-knob
  ``if "X" in kwargs:`` block).
- ``dorm._scoped.ScopedCollector`` — shared primitive for
  ``ContextVar``-based per-task signal collectors. Used by
  ``query_count_guard``, ``assertNumQueries`` and ``QueryLog``.
- ``LocMemCache`` carries a secondary ``defaultdict[prefix, set]``
  index — ``delete_pattern("ns:*")`` is now O(matches) instead of
  O(n).
- Sticky read-after-write window upgraded from copy-on-write to
  lazy-copy-then-mutate: each task takes one private dict on
  first write, then mutates in place for the rest of the request.
- ``QueryRecord`` and ``TemplateStats`` are now ``@dataclass(slots=True)``.
  ``QueryLog.summary()`` returns ``list[TemplateStats]``;
  consumers that read dict keys should switch to attribute access
  or call ``.to_dict()``.
- ``Settings._explicit_settings`` lives on the instance (set in
  ``__init__``) instead of as a class-level mutable default.
- Per-query counters in ``query_count_guard`` and
  ``assertNumQueries`` mutate a single-element ``list[int]`` in
  place instead of calling ``ContextVar.set`` per query — saves
  one ``Token`` allocation per signal.

### Tests

- ``tests/test_slow_query_setting_v2_6.py`` — ``SLOW_QUERY_MS``
  resolution order, ``None``-disables behaviour,
  ``configure``-driven cache invalidation, ``cacheable`` flag.
- ``tests/test_v2_6_features.py`` + ``test_v2_6_audit_fixes.py``
  + ``test_v2_6_improvements.py`` — query-count guard,
  ``assertNumQueries``, sticky window, lint rules, querylog,
  LocMemCache, row-cache, audit-fix regression tests, and the
  ``MemoizedSetting`` / ``ScopedCollector`` helper coverage.
- ``tests/test_v2_7_auth.py`` — password-hashing helpers + User /
  Group / Permission round-trip, key reuse, salt entropy,
  superuser-flag enforcement, group-permission resolution.
- ``tests/test_v2_7_asyncguard.py`` — guard activation modes,
  warn dedup, sync-context inertness, listener teardown.
- ``tests/test_v2_7_functions.py`` — SQL-shape pinning for every
  new function and re-export check against ``dorm.__all__``.
- ``tests/test_v2_7_parity.py`` — ``in_bulk``,
  ``Manager.from_queryset``, ``Prefetch(to_attr=…)`` and
  ``Meta.managed = False`` round-trips.
- ``tests/test_v2_7_bench_smoke.py`` — benchmark runner survives
  an end-to-end sqlite run.
- ``tests/test_v2_8_encrypted.py`` — round-trip, deterministic /
  random nonce semantics, key rotation, tamper detection,
  missing / invalid key handling, field-level prep / from_db_value
  path. Skipped when ``cryptography`` isn't installed.
- ``tests/test_v2_8_prometheus.py`` — install idempotence, query
  recording, exposition shape, label escaping, uninstall reset.

## [2.5.0] - 2026-05-02

Minor release. Two opt-in features land alongside the v2.4.1
bug-hunt corpus, plus a follow-up audit pass that closed five
issues (B1, B4, B5, B7, B9) found while writing coverage tests.

### Fixed — bulk write ops now invalidate the cache

- ``QuerySet.update`` / ``delete`` / ``bulk_create`` /
  ``bulk_update`` (and the async counterparts) bypass
  ``post_save`` / ``post_delete`` per row — the previous
  cache layer left every cached queryset on the model
  populated for the full TTL after a bulk write. Now each of
  these methods schedules an invalidation through the same
  on-commit hook the per-instance signal handlers use.
- No-op bulk calls (empty list to ``bulk_create`` /
  ``bulk_update``, zero-row update / delete) DO NOT churn the
  cache or bump the version counter.
- New helpers ``dorm.cache.invalidation.invalidate_model`` /
  ``ainvalidate_model`` for callers that route writes outside
  the standard QuerySet methods.

### Hardened — opt-in strict signing key for multi-worker

- Without ``CACHE_SIGNING_KEY`` / ``SECRET_KEY`` the cache
  layer falls back to a per-process random key. In
  multi-worker deployments this silently collapses the cache
  to per-worker visibility (other workers can't verify the
  signatures). New setting ``CACHE_REQUIRE_SIGNING_KEY`` (default
  ``False``) refuses the fallback and raises
  ``ImproperlyConfigured`` on first cache use — recommended
  for any production-shaped deployment.

### Fixed — cache key invariant under filter() kwarg ordering

- ``filter(a=1, b=2)`` and ``filter(b=2, a=1)`` produced
  different SQL → different cache keys → halved hit rate for
  semantically identical queries. The cache-key digest now
  hashes a CANONICAL representation of the queryset state
  (sorted leaf-condition tuples, positional ``order_by``,
  limit / offset / select_related / annotations / etc.) so
  iteration order doesn't perturb the digest. SQL emission is
  NOT sorted — a future query-plan tweak based on predicate
  order would break otherwise.

### Fixed — libsql async wrapper detects event-loop change

- ``LibSQLAsyncDatabaseWrapper`` stamped the loop on first
  open and reused the bound connection across every later
  call. Pytest-asyncio per-test loops + multi-loop ASGI
  workers triggered native crashes when a coroutine on a
  fresh loop reached into the prior loop's connection. The
  wrapper now checks the running loop on every ``_get_conn``
  call and resets cached state (async conn, sync conn,
  executor, lock) when the loop changed — next acquire opens
  fresh.

### Security — cache payloads now HMAC-signed

- ``pickle.loads`` over Redis bytes is RCE if the cache is
  reachable by an attacker (multi-tenant Redis, no-auth
  deployment). Every cached payload now ships with an
  HMAC-SHA256 signature header (``b"dormsig1:<hex64>:<pickle>"``).
  The signing key reads from ``settings.CACHE_SIGNING_KEY`` →
  ``settings.SECRET_KEY`` → a per-process random key (with a
  one-time warning so the operator knows the cache isn't
  shared across workers).
- Loads verify the signature with ``hmac.compare_digest`` BEFORE
  ``pickle.loads`` runs. Unsigned / tampered / truncated blobs
  are dropped silently — the queryset falls through to the
  database.
- New settings: ``CACHE_SIGNING_KEY`` (recommended),
  ``CACHE_INSECURE_PICKLE`` (default ``False``; opt-out for
  unsigned legacy caches you can't migrate).
- Helpers exposed: ``dorm.cache.sign_payload`` /
  ``dorm.cache.verify_payload`` for callers building custom
  cache backends or test harnesses.

### Fixed — Stale-read race between read and write

- The naïve "read → DB fetch → store" flow could cache a stale
  row if a concurrent ``Model.save()`` invalidated the key
  *between* the reader's fetch and store steps. Closed with a
  per-model in-memory version counter: every save / delete
  bumps it; ``_cache_key`` includes ``":vN:"``; ``_cache_store_*``
  re-reads the version after the DB fetch and stores under the
  (possibly bumped) key. A racing writer's bump now points
  later readers at the new key — the stale entry never gets
  written.
- New: ``dorm.cache.model_cache_version`` /
  ``dorm.cache.bump_model_cache_version``. Counter is
  process-local; cross-process coherence still goes through
  ``delete_pattern``.

### Fixed — `parse_database_url("libsql:////abs")` returned a relative path

- The libsql URL parser stripped one slash too many for the
  four-slash absolute form. ``libsql:////var/data/db.sqlite``
  produced ``var/data/db.sqlite`` — the open() landed next to
  the working directory instead of the intended ``/var/...``.
  Mirrors the sqlite branch's correct logic now: keep one
  leading slash for the absolute form, strip the lone slash
  for the relative one.

### Fixed — `ValuesListQuerySet._clone` / `CombinedQuerySet._clone` dropped cache state

- The base ``QuerySet._clone`` propagates ``_cache_alias`` /
  ``_cache_timeout``, but the two subclass overrides forgot.
  ``qs.cache().values_list("name").filter(active=True)`` lost
  caching on the second clone — silent miss every call.

### Fixed — `execute_script` async fallback corrupted quoted `;` literals

- The libsql async wrapper's ``executescript``-not-available
  fallback split on bare ``;`` — any DDL / DML containing a
  quoted ``;`` (``INSERT INTO t VALUES ('a;b')``, identifier
  ``"weird;name"``) got partitioned mid-literal and the
  resulting statements failed at parse time. Replaced with a
  shared quote-aware helper (``_split_statements`` in
  ``dorm/db/backends/sqlite.py``) that ignores ``;`` inside
  single- or double-quoted runs.

### libsql backend — earlier round of fixes

- async wrapper crashed on Python 3.14 + libsql_experimental
  due to native code issues and thread-safety violations
  (``asyncio.to_thread`` fans across multiple workers; libsql
  connections aren't thread-safe). Migrated to ``pyturso``
  (the official Turso Python SDK), pinned the async path to a
  single-thread ``ThreadPoolExecutor`` for remote / embedded-
  replica modes, and use ``turso.aio`` natively for local-only
  mode.
- ``LibSQLDatabaseWrapper.sync_replica`` raised
  ``ValueError: Sync is not supported in databases opened in
  Memory mode`` when called against a local-only wrapper —
  pyturso exposes ``conn.sync`` even for memory DBs but
  rejects the call. Now skipped when ``SYNC_URL`` isn't
  configured.
- Bind parameters: pyturso (and libsql_experimental) reject
  ``list``, accept only ``tuple`` / ``Mapping``. Both sync and
  async wrappers coerce in their execute-shaped methods.

### libsql backend — features

- **libsql backend** — talk to local files, remote
  Turso / sqld endpoints, or run as an embedded replica that
  syncs from a remote master. The dialect is SQLite-compatible,
  so the migration tooling and the SQLite branch of every
  compiler keep working untouched. Native vector support
  (``F32_BLOB(N)`` + ``vector_distance_l2`` /
  ``vector_distance_cos``) is wired into ``VectorField`` so
  embeddings round-trip without the sqlite-vec extension.

Both features are gated behind optional dependencies — install
``djanorm[libsql]`` and / or ``djanorm[redis]`` only when you
need them. ``djanorm`` itself imports without either client.

### Added — libsql backend (`dorm.db.backends.libsql`)

- ``ENGINE = "libsql"`` routes to ``LibSQLDatabaseWrapper``
  (sync) and ``LibSQLAsyncDatabaseWrapper`` (async). Three
  modes share a single configuration shape:
  - **Local file** — drop-in SQLite replacement.
  - **Self-hosted ``sqld`` (VPS)** — typical production layout.
    ``SYNC_URL`` (``https://...``) + ``AUTH_TOKEN`` connect to
    your own server.
  - **Embedded replica** — local file + ``SYNC_URL`` keeps the
    file in sync with the remote master. ``sync_replica()``
    on the wrapper triggers an explicit pull.
  - **Turso Cloud** — same wire protocol as self-hosted ``sqld``;
    point ``SYNC_URL`` at ``libsql://<db>-<org>.turso.io``.
- Powered by ``pyturso`` — the official Turso Python SDK.
  Local-only async uses ``turso.aio`` natively; embedded
  replica / remote-only async runs the sync client on a
  dedicated single-thread ``ThreadPoolExecutor`` (pyturso
  connections aren't thread-safe, so the default
  ``asyncio.to_thread`` pool would fan calls across multiple
  workers and produce native crashes).
- URL parser (``parse_database_url``) recognises ``libsql://``,
  ``libsql+wss://``, ``libsql+ws://``, ``libsql+http://`` and
  ``libsql+https://``. Auth tokens come from the ``authToken``
  query parameter; the optional ``NAME`` query parameter sets
  the embedded-replica file path.
- Client lookup raises ``ImproperlyConfigured`` pointing at
  ``pip install 'djanorm[libsql]'`` if pyturso isn't
  installed.

### Added — vector support on libsql

- ``VectorField.db_type`` returns ``F32_BLOB(N)`` for
  ``vendor == "libsql"``. The packed-float32 wire format
  (already used by sqlite-vec) is reused — libsql's
  ``vector32(?)`` SQL function reads it directly.
- ``L2Distance`` / ``CosineDistance`` compile to
  ``vector_distance_l2`` / ``vector_distance_cos`` against
  ``vector32(?)``. ``MaxInnerProduct`` raises
  ``NotImplementedError`` (libsql ships no negated-IP function;
  use ``CosineDistance`` over normalised embeddings instead).
- No ``VectorExtension()`` migration is needed on libsql —
  vector functions are built into the server.

### Added — Result cache (`dorm.cache`)

- ``QuerySet.cache(timeout=…, using="default")`` opts a
  queryset into result caching. Returns a clone (chaining is
  immutable, matching every other QuerySet API). Honoured by
  the sync iterator (``for x in qs``) and the async terminal
  (``await qs``).
- Cache key is a SHA-1 of ``(final SQL, bound params)``
  namespaced by ``f"dormqs:{app_label}.{ModelName}"`` so
  signal-driven invalidation can wipe every cached queryset for
  a model with a single ``delete_pattern`` call.
- ``BaseCache`` defines the contract every backend implements:
  ``get`` / ``set`` / ``delete`` / ``delete_pattern`` (sync) and
  the matching ``a*`` async variants. Both return raw bytes so
  the queryset layer can pickle / unpickle on its own.
- ``RedisCache`` (``dorm.cache.redis``) wraps redis-py for sync
  and ``redis.asyncio`` for async. Both pools are spun up
  lazily; ``LOCATION`` accepts every URL form redis-py knows.
  Every operation is wrapped in ``try / except`` — cache
  outages fall through to the DB silently, never propagate.
- Auto-invalidation hooks (``dorm.cache.invalidation``)
  connect ``post_save`` / ``post_delete`` (sync + async) on
  first ``qs.cache()`` call, so projects that never opt into
  caching pay zero dispatch cost.
- ``configure(CACHES={...})`` invalidates the memoised cache
  instances so a mid-process settings swap doesn't keep the
  old client alive.

### Added — settings

- ``settings.CACHES`` (default ``{}``). Same shape as Django:
  ``{alias: {"BACKEND": "dotted.path.Backend", "LOCATION": …,
  "OPTIONS": {…}, "TTL": 300}}``.
- ``settings.SEARCH_CONFIG`` (default ``"english"``) — already
  added in v2.4.1's R24 fix; mentioned here for completeness.

### Optional dependencies

- ``djanorm[libsql]`` → ``libsql-experimental``. Local file,
  embedded replica, or remote (Turso / sqld).
- ``djanorm[redis]`` → ``redis`` (sync + asyncio in one package).
- ``djanorm[pgvector]`` → ``pgvector`` only. The PostgreSQL
  psycopg adapter for ``VectorField``. **No longer pulls
  ``sqlite-vec``** — split into its own extra so a SQLite-only
  install doesn't drag in the PG adapter and vice-versa.
- ``djanorm[sqlite-vec]`` → ``sqlite-vec`` only. The SQLite
  loadable extension binary used by ``VectorExtension`` and
  ``VectorField`` on SQLite.
- ``djanorm[vector]`` (new convenience meta-extra) → pulls both
  ``[pgvector]`` and ``[sqlite-vec]`` for projects targeting
  mixed PG/SQLite deployments.
- ``djanorm[all]`` now pulls every extra alongside the existing
  set.

### Tests

- ``tests/test_libsql_v2_5.py`` — URL parsing, engine routing,
  local-file round-trip, vendor branch in ``VectorField`` /
  distance expressions, fallback error path when the client
  isn't installed.
- ``tests/test_redis_cache_v2_5.py`` — ``qs.cache()`` clone
  semantics, hit / miss / store, key namespacing, sync + async
  round-trip, signal-driven invalidation, cache-outage
  fallthrough, helpful error when redis-py is missing.

### Documentation

- ``docs/libsql.en.md`` / ``docs/libsql.es.md`` — full guide
  covering local / remote / embedded-replica modes, async
  usage, vector support, migrations and limitations.
- ``docs/cache_redis.en.md`` / ``docs/cache_redis.es.md`` —
  configuration, ``qs.cache(...)`` semantics, async path,
  auto-invalidation contract, custom backend protocol, when
  caching helps and when it hurts.

## [2.4.1] - 2026-05-01

Bug-hunt patch release. Fifty-three latent correctness / dataloss /
async-parity / migration-safety / security issues surfaced across
three multi-agent audit rounds, all closed with regression tests
in ``tests/test_bug_hunt_v2_5.py`` (round 1, 48 cases),
``tests/test_bug_hunt_v2_6.py`` (round 2, 46 cases) and
``tests/test_bug_hunt_v2_7.py`` (round 3, 74 cases). No public API
changes.

### Round 3 — Fixed (24 issues)

#### `filter(field=None)` returned 0 rows instead of NULL rows

- ``filter(deleted_at=None)`` emitted ``deleted_at = NULL`` (always
  FALSE in standard SQL) — silently dropping every row the user
  asked for. The query compiler now rewrites ``= None`` to
  ``IS NULL`` (and ``exclude(...=None)`` to ``IS NOT NULL``),
  matching Django's documented behaviour.

#### Sliced `update()` / `aupdate()` ignored LIMIT (silent dataloss)

- Sister bug to the round-1 sliced-``delete()`` fix. ``qs[:5].update
  (active=False)`` deactivated the entire filtered population, not
  the requested 5 rows. Both sync and async ``update`` now collect
  the bounded pks first and re-scope the UPDATE through
  ``WHERE pk IN (collected_pks)`` whenever a slice is active.

#### `_compile_expr` ignored `Subquery` / `Exists` / function expressions in update kwargs

- ``Book.objects.update(title=Subquery(...))`` bound the
  ``Subquery`` *object itself* as a parameter — the driver crashed
  with ``cannot adapt type 'Subquery'``. ``_compile_expr`` now
  routes any expression with an ``as_sql`` method through its own
  emitter and threads the outer table alias / model so embedded
  ``OuterRef`` references resolve.

#### `_is_unsaved` treated `Model(pk=0)` as already-saved

- ``Model(pk=0).save()`` (and any other DB-controlled-but-falsy
  pk) routed through UPDATE, affected zero rows, and returned
  silently — no insert, no error. Adopted Django's ``_state.adding``
  flag: True on fresh instances, False after a successful INSERT
  or DB hydration. The legacy ``pk is None`` heuristic remains as
  a fallback for instances that predate ``_state``.

#### `_adapt_placeholders` corrupted `%s` inside SQL string literals

- The PG ``%s`` → ``$N`` rewrite ran a naive ``re.sub`` over the
  full SQL — including literals. ``WHERE name = 'foo%s_bar'``
  became ``'foo$1_bar'``, breaking ``RawQuerySet`` and any LIKE
  pattern containing the placeholder sequence as part of the
  string. The rewrite now tokenises quoted runs (with ``''``
  escaping) and only renumbers bare ``%s`` between them.

#### `only().defer()` chain undid the projection restriction; `db_column` ignored

- ``only("a", "b").defer("a")`` widened the SELECT back to
  "all columns minus a", silently undoing the ``only()`` step.
  ``own_defer`` also held *attnames* but compared them against
  *column names*, so ``defer("name")`` with
  ``db_column="display_name"`` was a no-op. Both fixed.

#### `bulk_create(unique_fields=…)` ignored `db_column` overrides

- ``unique_fields=["external_id"]`` interpolated the attname
  verbatim into ``ON CONFLICT (...)`` — even when the field
  declared ``db_column="ext_uid"``. PG raised *no unique
  constraint matching the columns*. ``unique_fields`` now
  resolves through ``meta.get_field(...).column`` like
  ``update_fields`` already did.

#### `values()` / `values_list()` did not emit JOINs for FK traversal

- ``qs.values("publisher__name")`` stored ``"publisher__name"``
  literally in ``selected_fields``; ``get_columns`` then emitted
  it as a plain column reference with no JOIN — crashing or
  pulling the wrong column. The compiler now resolves dotted
  paths through ``_resolve_column`` (registering the JOIN) and
  aliases the projection back to the user-visible dotted name so
  ``row["publisher__name"]`` Just Works.

#### `OneToOneField` had no reverse descriptor

- ``OneToOneField`` inherited from ``RelatedField`` directly, not
  ``ForeignKey``, so the reverse-side wiring in
  ``ForeignKey.contribute_to_class`` never ran.
  ``target_instance.<related_name>`` raised ``AttributeError``.
  Added a dedicated ``ReverseOneToOneDescriptor`` that returns
  the single related instance (or raises ``DoesNotExist``),
  caches the result on the source instance, and supports
  reverse-side assignment.

#### `BinaryField` returned `memoryview` from PostgreSQL

- psycopg adapts ``BYTEA`` to ``memoryview``; without a
  ``from_db_value`` override the field's ``Field[bytes]``
  annotation lied and ``obj.data.startswith(b"\x89")`` raised
  ``AttributeError``. Now coerces every db-returned value to
  ``bytes``.

#### `VectorField.from_db_value` accepted wrong-dimension vectors

- The write path enforced ``len(seq) == dimensions``; the read
  path didn't. A corrupted column or a cross-dimension
  migration silently round-tripped the wrong shape. Now raises
  ``ValidationError`` at hydration.

#### `annotate()` collided with field names without warning

- ``annotate(name=Count("books"))`` on a model with a ``name``
  column emitted SELECT with two ``"name"`` outputs and hydrated
  whichever the driver kept. Django raises ``ValueError`` here;
  djanorm now does too. ``alias()`` retains the legacy
  field-shadow behaviour because alias-only names never reach
  the SELECT list.

#### `NPlusOneDetector` template normalisation missed several literal shapes

- Negative numbers (``-5``), hex literals (``0xABCD``), scientific
  notation (``1.5e10``) and PG byte strings (``X'…'``) were not
  collapsed. Mixed-sign loops and any of those literal shapes
  produced multiple templates per N+1 pattern, slipping past the
  detector. Patterns extended to cover them.

#### `Q.__invert__` shared mutable nested children

- ``q = Q(a=1) & Q(b=2); ~q`` returned a Q whose nested children
  were the *same instances* as the original's; mutating one
  bled to the other. Now deep-copies every nested Q wrapper.
  Tuple children stay shared (tuples are immutable).

#### Self-correlated subqueries (``Exists(SameModel.filter(pk=OuterRef("pk")))``) had alias collisions

- Both outer and inner used ``alias = table``, so the
  ``OuterRef`` reference was ambiguous on PG and silently bound
  to the inner row on SQLite. Self-correlated subqueries now
  alias the inner side as ``"<table>_sub"``; ``_resolve_column``
  honours the per-query ``_self_alias`` so inner column refs
  qualify correctly.

#### `Manager.from_queryset` shadowed user QuerySet overrides

- The proxy generator skipped any QS method whose name appeared
  on ``BaseManager``, including the user's own override of
  ``count`` / ``filter`` / ``update`` etc. A custom
  ``def update(self, *, dry_run=False, **kw)`` on the QuerySet
  was unreachable through the manager. Reflection now always
  proxies methods declared directly on the user's QuerySet
  class, even when the name collides with a BaseManager proxy.

#### `configure(DATABASES=...)` did not invalidate cached connections

- A second ``configure(DATABASES={"default": cfg_b})`` kept the
  ``cfg_a`` wrapper alive in ``_sync_connections`` /
  ``_async_connections``. Subsequent queries silently hit the
  previous backend. ``configure`` now calls
  ``reset_connections`` whenever ``DATABASES`` changes (skipped
  when only ``STORAGES`` etc. is updated).

#### `Meta.default_manager_name` was silently ignored

- The metaclass always picked the first declared manager.
  ``Meta.default_manager_name`` is now honoured: the named
  manager becomes ``_default_manager``; an unknown name raises
  ``ImproperlyConfigured`` so the typo surfaces immediately.

#### Signal connect / disconnect collided on bound-method ids

- ``id(obj.method)`` returns the id of a *temporary* bound-method
  that gets GC'd as soon as ``connect`` returns. CPython
  recycles those ids freely, so a subsequent
  ``connect(other_obj.method)`` could produce the same id and
  silently disconnect the first receiver. Both ``connect`` and
  ``disconnect`` now key bound methods on a stable composite
  ``(id(obj), id(func))`` uid.

#### Inherited ``Manager`` subclasses with ``__init__`` args crashed at child-model definition

- Re-instantiation via ``mgr.__class__()`` required a zero-arg
  constructor — custom ``class TenantManager(Manager): __init__
  (self, tenant)`` raised ``TypeError`` the moment a child model
  was defined. Now uses ``copy.copy`` to clone the parent's
  manager instance, preserving constructor args (and any
  post-init attributes) without re-running ``__init__``.

#### `DurationField._parse_iso8601` rejected `str(timedelta)` for negatives

- Python's ``str(timedelta(hours=-1, minutes=-30))`` is
  ``"-1 day, 22:30:00"``. The previous parser accepted only
  ``HH:MM:SS[.ffffff]``; any negative interval read back via
  ``str(td)`` raised ``ValidationError``. Now strips the
  ``"-N day(s),"`` prefix and applies the offset.

#### ContentType cache survived test teardown / re-migration windows

- The ``(app_label, model)`` → instance cache lived in a
  ``ClassVar`` dict. Tests that truncate the table and re-migrate
  inside the same process kept getting the old cached row, whose
  pk no longer existed. ``get_for_id`` now evicts the cache
  entry on ``DoesNotExist`` so the next ``get_for_model`` call
  rebuilds from the live table; ``reset_connections`` also
  clears the cache as a belt-and-braces measure.

#### `GenericForeignKey.for_concrete_model` flag was a documented no-op

- The constructor stored the flag, ``__set__`` ignored it. With
  proxy / multi-table inheritance the GFK stored the
  *subclass* CT instead of the concrete parent (Django's
  default), breaking polymorphic queries that filter by
  concrete CT. Now resolves through
  ``type(value)._meta.concrete_model`` when the flag is set.

#### `__search` lookup hardcoded the ``'english'`` text-search dictionary

- Spanish / multi-lingual apps couldn't configure the dictionary
  via ``filter(title__search="…")`` — they had to drop to
  ``SearchQuery`` directly. Now reads from
  ``settings.SEARCH_CONFIG`` (default ``"english"``) and
  validates the value as a SQL identifier before splicing.

### Earlier rounds — see entries below for rounds 1 and 2.



### Fixed — `Aggregate(filter=Q(...))` was accepted but silently ignored

- ``Sum("amount", filter=Q(status="paid"))`` stored ``filter`` on
  the instance but ``as_sql`` never referenced it, so the aggregate
  summed every row regardless of the predicate. Reporting code
  silently produced wrong totals — worst-class bug. The compiler
  now emits ``FILTER (WHERE …)`` on PostgreSQL and wraps the
  expression in a ``CASE WHEN … THEN expr END`` on SQLite (so the
  aggregate skips non-matching rows there too).

### Fixed — `_compile_subquery` (`__in qs`) dropped JOINs, ignored `.values()`

- Three failures on the same site:
  - FK-traversal in the inner queryset
    (``Book.objects.filter(genre__name="x")``) registered a JOIN
    that the bare ``SELECT pk FROM table WHERE …`` form discarded,
    crashing with *missing FROM-clause entry* on PG / *no such
    column* on SQLite.
  - ``parent.filter(child__in=Book.objects.values("author_id"))``
    always projected the model PK regardless of ``.values()`` —
    the comparison was silently against the wrong column.
  - Annotations / vendor-aware date-part lookups inside the inner
    queryset were dropped along with their JOINs.
  All three preserved now: WHERE compiles first so joins are
  registered, ``selected_fields`` chooses the projected column,
  and the JOIN clauses follow the SELECT.

### Fixed — `_compile_condition` dropped FK-traversal segments + assumed SQLite vendor

- ``Q(author__name="x")`` inside ``When(...)`` / ``CheckConstraint``
  / partial-index predicates emitted ``"name" = %s`` against the
  *current* table — the ``author`` segment silently disappeared
  and the SQL referenced a non-existent column. ``__year``, ``__date``,
  ``__in`` and friends always took the SQLite branch because
  ``build_lookup_sql`` was called without ``vendor=``, so PG
  ``CHECK`` constraints with date-part predicates raised
  ``function strftime(unknown, timestamp) does not exist``.
  ``_compile_condition`` now walks every relation hop and threads
  the resolved vendor (or an explicit ``vendor=`` kwarg) through to
  the lookup layer.

### Fixed — `Concat(F('a'), F('b'))` returned NULL when any operand was NULL

- The ``a || b`` expansion poisoned the result when *any* operand
  was NULL on both PG and SQLite, where Django's ``Concat`` skips
  NULLs and returns the concatenation of the non-NULL parts. Each
  operand is now wrapped in ``COALESCE(expr, '')`` so a NULL
  contributes the empty string. ``filter(full__contains="Smith")``
  used to silently drop every row where any source column was
  NULL; now matches the documented behaviour.

### Fixed — `__in` with a generator (or any non-sized iterable) raised `TypeError`

- ``filter(id__in=(x.pk for x in some_iter))`` crashed with ``object
  of type 'generator' has no len()`` because ``len(value)`` ran on
  the un-materialised iterable. The lookup builder now materialises
  ``value`` once into a list — generators, sets, dict_values, and
  the rest of the iterable hierarchy all work.

### Fixed — `__regex` / `__iregex` raised on PostgreSQL

- Templates emitted SQLite's ``REGEXP`` keyword on every backend.
  PG raised ``syntax error at or near "REGEXP"``. The lookup is
  now vendor-aware: PG uses ``~`` (case-sensitive) and ``~*``
  (case-insensitive), SQLite keeps ``REGEXP``.

### Fixed — `migrate(dry_run=True)` wrote to the migration recorder

- ``_sync_squashed`` ran *before* ``_apply_forward`` swapped in the
  dry-run capture proxy, so it called ``self.recorder.record_applied``
  through the real connection. The docstring promised "the migration
  recorder is **not** updated"; now true: the squashed-sync step is
  skipped entirely on dry-run.

### Fixed — `FileSystemStorage` allowed symlink-based sandbox escape

- ``_resolve_path`` collapsed ``..`` segments lexically (``abspath``)
  but did NOT follow symlinks. A symlink stored inside the storage
  root (``media/escape -> /etc``) passed the prefix check while the
  subsequent ``open()`` followed the link out of the sandbox. Both
  sides of the comparison now use ``os.path.realpath``, which
  resolves symlinks too.

### Fixed — `dorm migrate <app> <missing_target>` exited 0

- The ``except ValueError`` branch in ``cmd_migrate`` printed an
  error and continued; the loop completed and the process exited
  with status 0. CI gating on ``dorm migrate`` couldn't catch a
  missing / invalid target. Now ``sys.exit(1)`` after the error
  message — same exit-code contract as ``dorm dbcheck``.

### Fixed — `serialize.load` couldn't load fixtures with forward FK references

- The single insertion loop walked records in fixture order. Self-
  referential FKs and cyclic graphs (the canonical cases ``dumpdata``
  itself can produce) hit ``IntegrityError: FK violates`` because
  the parent row appeared after its child. ``load`` now defers FK
  validation for the duration of the txn:
  ``SET CONSTRAINTS ALL DEFERRED`` on PostgreSQL (when user FKs are
  ``DEFERRABLE``) and ``PRAGMA defer_foreign_keys=ON`` on SQLite.

### Fixed — `dorm inspectdb` produced unimportable Python for legacy schemas

- A column named ``from`` / ``class`` / ``order`` (legitimate in
  legacy databases, reserved words in Python) emitted
  ``from = dorm.TextField()`` — ``SyntaxError`` at import. The
  inspector now sanitises every attribute name through
  ``keyword.iskeyword`` / ``str.isidentifier`` and pins the original
  column with ``db_column=`` so the runtime mapping is preserved.

### Fixed — `get_available_name` produced names violating `max_length`

- When the random-suffix retry path triggered with a tight
  ``max_length``, ``stem[:-cut]`` could collapse to ``""`` (when
  ``cut > len(stem)``) and the resulting ``"_<token>.ext"`` could
  itself exceed ``max_length``. The shrink path now floors the
  stem at length 0, drops the leading underscore when the stem
  doesn't survive, and clips the token as a last resort so the
  return value always honours ``max_length``. The matching ``_save``
  path also opens with ``O_CREAT | O_EXCL`` to surface a clear
  ``FileExistsError`` instead of silently overwriting an existing
  file when two writers raced past the prior ``exists()`` probe.

### Fixed — `set_autocommit` only affected the calling thread's connection

- The SQLite wrapper toggled ``isolation_level`` on the calling
  thread's thread-local connection but left every other thread's
  cached connection on the previous setting. A wrapper put into
  autocommit mode by Thread A would still wrap Thread B's writes
  in implicit BEGIN — split-brain. The setter now walks every live
  connection in ``self._conns`` under the lock and applies the new
  isolation level.

### Fixed — async PG pool from a dead event loop leaked libpq sockets

- ``_get_pool`` cleaned up after a loop-change by scheduling
  ``pool.close()`` on the old loop, but only when the old loop was
  still running. When the old loop was already closed (the common
  case under ``pytest -n 4`` event-loop cycling), the pool's
  ``__del__`` ran later on a dead loop and could SIGSEGV the
  worker. The dead-loop branch now mirrors ``force_close_sync``:
  walk the pool's idle deque and call ``pgconn.finish()`` on every
  libpq connection so the pool is inert by the time the GC reaches
  it.

### Fixed — `count()` / `exists()` / `update()` / `delete()` ignored FK-traversal JOINs

- ``Book.objects.filter(author__name="x").count()`` (and the
  matching ``exists`` / ``update`` / ``delete`` calls) used to
  emit ``SELECT … FROM "books" WHERE "books_author"."name" = ?``
  with no JOIN clause, crashing with *no such column*. The four
  emitters now compile WHERE *first* (so ``_resolve_column``
  populates ``self.joins``), then attach the JOINs. ``UPDATE`` /
  ``DELETE`` use a portable ``WHERE pk IN (SELECT pk FROM t
  JOIN … WHERE …)`` rewrite — both PG and SQLite accept it.

### Fixed — sliced `delete()` / `adelete()` wiped the full filtered set

- ``qs[:5].delete()`` collected 5 PKs through ``values_list``
  but the final ``DELETE`` ran against the original ``where_nodes``
  with no LIMIT, silently deleting the entire matching population
  (dataloss). When ``limit_val`` or ``offset_val`` is set, both
  the sync and async ``delete`` now re-scope through ``pk__in=…``
  using the already-collected list.

### Fixed — nullable FK joins were always INNER (broke `__isnull` semantics)

- Any FK traversal in ``filter()`` / ``order_by()`` registered an
  ``INNER JOIN``. For nullable FKs that meant
  ``filter(publisher__name__isnull=True)`` excluded the very
  rows the user asked for. ``_resolve_column`` now emits ``LEFT
  OUTER JOIN`` when the FK is nullable, ``INNER`` otherwise —
  matching Django's default and preserving null-side rows.

### Fixed — `AlterField` SQLite rebuild lost `Meta.constraints` and FK references

- The rebuild recipe re-emitted ``Meta.indexes`` but never
  ``Meta.constraints`` — ``CheckConstraint`` /
  ``UniqueConstraint`` silently disappeared on every alter.
  ``PRAGMA defer_foreign_keys=ON`` is now issued before the
  drop+rename so child FK references stay valid through the txn
  (``PRAGMA foreign_keys=OFF`` is a no-op inside an open
  transaction; deferred FK-check is the right primitive here).
  A post-rebuild ``PRAGMA foreign_key_check`` raises if the
  rebuild left dangling references, so a corrupted schema rolls
  back instead of being committed.

### Fixed — Migration writer DDL `DEFAULT` did not escape single quotes

- ``CharField(default="O'Brien")`` emitted ``DEFAULT 'O'Brien'``
  — broken DDL and an injection vector if the default ever
  derived from anything user-influenced. The string-default
  branch now escapes via the SQL-standard ``'`` → ``''``
  doubling.

### Fixed — Async iterator never hydrated annotation values onto instances

- The sync ``_iterator`` writes
  ``instance.__dict__[alias] = row[alias]`` for every queryset
  annotation; the async ``_aiterator`` skipped that block, so
  ``[a async for a in Author.objects.annotate(n=Count("book"))]``
  produced instances without ``.n``. Async path now mirrors
  sync.

### Fixed — Async `Prefetch(queryset=…)` was unusable

- ``_ado_prefetch_related`` passed the raw ``Prefetch`` object
  to ``_meta.get_field`` and crashed with ``TypeError`` /
  ``FieldDoesNotExist``. Async prefetch now normalises the spec
  to ``(lookup, queryset, to_attr)`` and propagates both kwargs
  to every async helper (``_aprefetch_m2m`` /
  ``_aprefetch_generic_relation`` /
  ``_aprefetch_reverse_fk``). ``_aprefetch_gfk`` keeps the same
  ``user_qs`` / ``to_attr`` rejections the sync helper has.

### Fixed — `aget_queryset` ignored prefetch cache (silent N+1 in async)

- After ``async for a in Article.objects.prefetch_related("tags")``,
  calling ``await a.tags.aget_queryset()`` re-queried the DB
  even though the prefetch had already populated
  ``_prefetch_tags``. The async manager now mirrors the sync
  cache lookup, returning a ``QuerySet`` with
  ``_result_cache`` pre-populated.

### Fixed — `adelete` ran reverse-FK cascades through the synchronous code path

- ``adelete`` called ``_handle_on_delete`` which iterated
  reverse-FK descriptors with ``obj.delete()`` /
  ``related_qs.update(...)`` — blocking SQL inside the event
  loop. A new ``_ahandle_on_delete`` runs every CASCADE /
  ``SET_NULL`` / ``SET_DEFAULT`` step through the async
  queryset (``adelete`` / ``aupdate``).

### Fixed — `_handle_on_delete` only walked `type(self).__dict__`

- Models inheriting from another concrete model installed their
  reverse-FK descriptors on the parent class. Cascade handling
  on a child instance silently skipped those parent descriptors.
  Both the sync and async cascade handlers now walk the MRO via
  a shared ``_iter_reverse_fk_descriptors`` helper.

### Fixed — M2M `add` / `remove` / `set` / `clear` left stale prefetch cache

- After ``prefetch_related("tags")`` a subsequent
  ``art.tags.add(tag2)`` did not clear ``_prefetch_tags``, so
  ``art.tags.all()`` returned the *pre-mutation* list. ``set()``
  was worst: it diffed against the stale cache and computed the
  wrong INSERT/DELETE. Every mutation method (sync + async) now
  drops the cache slot via a shared
  ``_invalidate_prefetch_cache`` helper, and ``set()`` /
  ``aset()`` invalidate before reading current state.

### Fixed — Autodetector `&` and `-` precedence quietly absorbed renamed fields

- ``set(from_fields) & set(to_fields) - renamed_old_fields``
  parses as ``a & (b - c)`` in Python — the subtraction ran
  first against ``to_fields`` (where renamed-from names aren't
  present) and never excluded them from the AlterField loop.
  Now explicitly parenthesised:
  ``(set(from_fields) & set(to_fields)) - renamed_old_fields``.

### Fixed — Field-rename heuristic compared only `db_type`

- The heuristic that auto-detects ``RenameField`` matched on
  ``db_type`` alone — same flaw the AlterField branch was
  patched for. ``VectorField(384)`` and ``VectorField(1536)``
  both render ``BLOB`` on SQLite, so a simultaneous rename +
  dimension change registered as a *pure* rename and dropped
  the type change. Now compares ``db_type`` AND the writer's
  serialised output.

### Fixed — `_serialize_field` exception silently masked AlterField changes

- A single ``try/except`` set ``old_s = new_s = None`` when
  *either* serialise call raised, and the equality check then
  evaluated ``None != None`` → False, so a real change was
  treated as a no-op. The two sides are now serialised
  independently; an asymmetric failure (one side raises, the
  other succeeds) is treated as *changed* — a spurious
  ``AlterField`` is strictly better than a silently-missed
  one.

### Fixed — M2M `add` / `aadd` had a SELECT-then-INSERT race window

- The two-query batching (``SELECT existing pks`` then
  ``INSERT to_add``) ran outside any transaction. Two concurrent
  ``add()`` calls could both observe ``existing=∅`` and both
  INSERT the same target rows, producing duplicates (or
  ``IntegrityError`` on UNIQUE through tables). Both sync and
  async paths now wrap the SELECT+INSERT pair in
  ``atomic`` / ``aatomic``.

### Fixed — `CombinedQuerySet` (UNION/INTERSECT/EXCEPT) ORDER BY skipped identifier validation

- Every other ORDER BY emitter validates field names via
  ``_validate_identifier``; the combinator path was the lone
  gap. A user-controlled ``order_by`` value forwarded from an
  API query string could inject SQL through this path. Now
  validates before quoting.

### Tests

- ``tests/test_bug_hunt_v2_5.py`` — 50 cases (48 active, 2
  PostgreSQL-only skipped without Docker) covering every round-1
  fix. Split sync / async where applicable; runs on both SQLite
  and PostgreSQL.
- ``tests/test_bug_hunt_v2_6.py`` — 46 cases (44 active, 2
  PostgreSQL-only skipped without a live PG server) covering every
  round-2 fix. Source-level checks are used where a live
  integration scenario would require simulating a closed event
  loop or a fully-mocked CLI plumbing chain.

## [2.4.0] - 2026-04-30

Big release. Four DX features (N+1 detector, GenericRelation
prefetch, only/defer with select_related, Pydantic Create/Update
schemas), full **vector search** support (pgvector + sqlite-vec)
under one ``dorm.contrib.pgvector`` module, and a chain of latent
bug fixes the new test passes surfaced.

### Added — Vector search (`dorm.contrib.pgvector`)

Same module covers both backends:

| Backend       | Column type   | Distance functions                     |
|---------------|--------------|----------------------------------------|
| PostgreSQL    | ``vector(N)`` | ``<->`` / ``<=>`` / ``<#>`` operators  |
| SQLite        | ``BLOB``      | ``vec_distance_L2`` / ``vec_distance_cosine`` |

- **`VectorField(dimensions=N)`** — declares the column. Vendor-aware
  ``db_type`` (``vector(N)`` on PG, ``BLOB`` on SQLite) and
  ``get_db_prep_value`` (text on PG, packed little-endian float32
  on SQLite). Validates length on write; round-trips
  ``list[float]`` / ``tuple`` / ``numpy.ndarray`` / pgvector's
  ``Vector`` / SQLite BLOB / memoryview on read.
- **`L2Distance` / `CosineDistance` / `MaxInnerProduct`** — distance
  expressions that compile per-vendor: pgvector operators on PG,
  ``vec_distance_*`` calls on SQLite. Compose with
  ``annotate()`` + ``order_by()`` for kNN. ``MaxInnerProduct``
  raises ``NotImplementedError`` on SQLite (sqlite-vec ships no
  negated-IP function — use ``CosineDistance`` over normalised
  embeddings instead).
- **`HnswIndex` / `IvfflatIndex`** — index helpers for the two
  pgvector ANN methods. ``opclass=`` defaults to
  ``vector_l2_ops``; method-specific storage parameters
  (``m=``, ``ef_construction=``, ``lists=``) flow through to a
  ``WITH (k = v, …)`` clause via the new ``Index.with_options``
  hook. (Vector indexes on SQLite require sqlite-vec's ``vec0``
  virtual table — not yet wrapped.)
- **`VectorExtension`** migration operation:
  - PG → ``CREATE EXTENSION IF NOT EXISTS "vector"`` forwards,
    ``DROP EXTENSION IF EXISTS "vector"`` backwards.
  - SQLite → loads sqlite-vec into the migration's connection AND
    flips ``SQLiteDatabaseWrapper._vec_extension_enabled`` so every
    future connection auto-loads the extension.
- **`load_sqlite_vec_extension(conn)`** — public helper for manual
  loading from app boot / ASGI lifespan / worker startup.
- **`dorm makemigrations --enable-pgvector <app>`** — CLI flag that
  writes the boilerplate migration calling ``VectorExtension()``.
- **`djanorm[pgvector]` extra** — installs both the ``pgvector``
  Python package (psycopg adapter) and ``sqlite-vec`` (loadable
  extension binary) so a single install line covers both backends.
- Step-by-step documentation at [`docs/pgvector.md`](docs/pgvector.en.md)
  with separate sections for the PostgreSQL and SQLite paths.

### Added — N+1 detector

- **`dorm.contrib.nplusone.NPlusOneDetector`** — context manager
  that hooks ``pre_query``, normalises every executed SQL to a
  parameter-stripped template, counts hits per template, and (in
  strict mode) raises ``NPlusOneError`` at exit when a template
  fired more than ``threshold`` times. ``raise_on_detect=False``
  produces a non-fatal report via ``detector.report()`` for
  staging-style auditing.
- **`assert_no_nplusone()`** — pytest-friendly helper.
  ``NPlusOneError`` subclasses ``AssertionError`` so pytest's
  traceback rewriting kicks in.
- DDL noise (``CREATE`` / ``DROP`` / ``ALTER`` / ``PRAGMA``,
  transaction control) filtered by default; caller can override
  via ``ignore=``.
- Identifier characters (``"authors"."id"``) preserved during
  normalisation; only string / numeric / NULL **literals**
  collapse to ``?`` so unrelated queries don't bucket together.

### Added — `prefetch_related` on reverse `GenericRelation`

- **Reverse polymorphic relations now batch.**
  ``Article.objects.prefetch_related("tags")`` resolves every tag
  pointing at every article in a single SELECT
  (``content_type = ct AND object_id IN (…)``), groups by
  ``object_id``, and stamps each article's manager cache. The
  ``GenericRelation`` manager's ``.all()`` reads from that cache
  before falling back to the live query — same contract as
  Django's prefetch + reverse-FK relation.
- Async parity via ``_aprefetch_generic_relation``.
- ``Prefetch("tags", queryset=…)`` honoured — the user-supplied
  filters / ordering / select_related are AND-ed onto the
  ``content_type`` predicate.

### Added — `only()` / `defer()` compose with `select_related`

- **Dotted paths now restrict the related projection.**
  ``Author.objects.select_related("publisher").only("name", "publisher__name")``
  emits a single LEFT OUTER JOIN that pulls only ``"authors"."id"``,
  ``"authors"."name"``, ``"publishers"."id"``, and
  ``"publishers"."name"`` — previously the projection-restriction
  short-circuited the JOIN entirely, silently degrading to a
  per-row N+1 on the related side.
- PK is always implicit so the hydrated related instance keeps a
  valid identity even when only non-PK columns were listed.
- ``defer("publisher__bio")`` is the inverse: keeps every related
  column except the named ones.
- Bare names (``only("name")``) keep their legacy semantics:
  parent-only restriction. Mixed (``only("name").defer("publisher__bio")``)
  works because the two sets live in separate state buckets.

### Added — Pydantic Create / Update schema helpers

- **`create_schema_for(model)`** — drops auto-incrementing PKs and
  ``GeneratedField`` columns automatically (server-controlled),
  keeps required fields required, propagates real defaults. Plain
  ``BaseModel`` subclass suitable as a FastAPI request body.
  Default class name ``f"{Model.__name__}Create"``.
- **`update_schema_for(model)`** — every remaining column becomes
  ``T | None`` with default ``None`` (PATCH semantics). Built via
  ``pydantic.create_model`` directly so the field-default
  propagation can be neutralised — a column with
  ``default=False`` mustn't advertise that default to the client
  when partial-update semantics say "no change". Constraint
  translation (``max_length``, ``ge=0``, ``Literal[…]`` for
  ``choices``, …) still applies — PATCH bodies aren't a free pass.
- Both helpers accept ``name=``, ``exclude=`` (extends the auto-PK
  drop), and ``base=`` (custom ``BaseModel`` ancestor).

### Fixed — `select_related` + `only()` was silently a no-op

- The SQL builder used to short-circuit the ``select_related`` JOIN
  whenever ``only()`` / ``defer()`` was active, on the (incorrect)
  assumption that the restricted projection would conflict with
  the aliased SR columns. The aliased columns
  (``"_sr_<path>_<col>"``) are namespaced by construction; emitting
  both is safe.
- ``ORDER BY`` now qualifies the parent column with the table name
  whenever any JOIN is in flight (WHERE-derived **or**
  select_related). Previously it qualified only when ``self.joins``
  was truthy and missed the SR case, producing ``ambiguous column
  name: id`` once a parent ``"id"`` and a related ``"id"`` both
  appeared unqualified.

### Fixed — `select_related` + WHERE column shared with related table

- WHERE column now qualified when ``select_related`` is active.
  ``Author.objects.filter(name="x").select_related("publisher")``
  previously emitted ``WHERE "name" = …`` without a table prefix —
  PostgreSQL raised ``column reference "name" is ambiguous`` and
  SQLite silently picked the parent's column. ``_resolve_column``
  now treats both ``self.joins`` and ``self.select_related_fields``
  as triggers for qualification.

### Fixed — `DecimalField` returned `float` on SQLite

- ``DecimalField.from_db_value`` coerces the cursor's value to
  ``decimal.Decimal``. SQLite stores NUMERIC with REAL affinity,
  so ``sqlite3`` returned floats — the field's annotation
  promised ``Decimal`` but the runtime value didn't match,
  breaking arithmetic with ``TypeError: unsupported operand
  type(s) for +: 'float' and 'decimal.Decimal'``. PG's psycopg
  adapter already returned ``Decimal``; an ``isinstance`` guard
  keeps that path zero-cost.

### Fixed — Migration autodetector missed non-type-changing field edits

- The autodetector compared only ``field.db_type(connection)``,
  which collapses different dimensions of a SQLite VectorField to
  the same ``BLOB`` (and similarly drops nullability / default /
  ``max_length`` tweaks for fields whose SQL type stays the same).
  The diff now also compares the writer's serialised output, so
  any edit the migration writer would emit differently triggers
  an ``AlterField``.

### Fixed — `AlterField` on SQLite was a no-op

- SQLite's ``ALTER TABLE`` doesn't support ``ALTER COLUMN``, so
  the operation silently did nothing. ``AlterField.database_forwards``
  now follows SQLite's `recommended rebuild recipe
  <https://www.sqlite.org/lang_altertable.html#otheralter>`_:
  create a new table with the up-to-date schema, copy the column
  intersection, drop the old, rename, and recreate any
  ``Meta.indexes`` declared on the model. PG path now also flips
  nullability (``DROP NOT NULL`` / ``SET NOT NULL``) on top of the
  existing ``ALTER COLUMN TYPE``.

### Fixed — Migration writer dropped `VectorField(dimensions=…)`

- The writer's ``_serialize_field`` had no branch for
  ``VectorField``, so generated migrations emitted
  ``VectorField()`` and crashed at import with
  ``TypeError: __init__() missing 1 required positional argument:
  'dimensions'``. The branch + the matching ``_FIELD_IMPORTS``
  entry are now in place.

### Tests

- ``tests/test_pgvector.py`` — ~100 cases. Unit-level field /
  expression / index helper coverage that runs on plain SQLite
  without any extension; integration cases that auto-skip when
  pgvector / sqlite-vec aren't loaded but verify round-trip + kNN
  ordering + index DDL when they are.
- ``tests/test_bug_hunt_v2_4.py`` — 80 cases targeting historically
  bug-prone ORM patterns (empty-input boundaries, NULL FK +
  ``select_related``, Q-object identities, Decimal precision
  round-trip, ``get_or_create`` / ``update_or_create`` semantics,
  CASCADE depth, NULL ordering, unicode / SQL-special characters,
  M2M idempotency, transaction rollback nesting, F-expression
  equality, UNIQUE conflict → ``IntegrityError``, FK column
  ordering). Both the Decimal and ambiguous-column bugs above
  were caught by tests in this file.

### Added — N+1 detector

- **``dorm.contrib.nplusone.NPlusOneDetector``** — context manager
  that hooks the :data:`pre_query` signal, normalises every executed
  SQL to a parameter-stripped template, counts hits per template,
  and (in strict mode) raises :class:`NPlusOneError` at exit when a
  template fired more than ``threshold`` times. Strict mode is the
  default; ``raise_on_detect=False`` produces a non-fatal report
  via ``detector.report()`` for staging-style auditing.
- **``assert_no_nplusone()``** — pytest-friendly helper that wraps
  the detector in strict mode and surfaces the violation as a
  regular ``AssertionError`` (so pytest's traceback rewriting
  works). Designed for use inside individual test functions.
- **DDL noise filtered by default** — ``CREATE`` / ``DROP`` /
  ``ALTER`` / ``PRAGMA`` / transaction control statements are
  ignored so test-fixture bookkeeping doesn't trip the detector.
  Caller can override via ``ignore=`` for custom suppression.
- Identifiers (``"authors"."id"``) are preserved verbatim during
  normalisation; only string / numeric / NULL **literals** get
  collapsed to ``?``. This keeps unrelated queries from accidentally
  bucketing together while still catching parameter-only variations
  of the same shape.

### Added — `prefetch_related` on reverse `GenericRelation`

- **Reverse polymorphic relations now batch.**
  ``Article.objects.prefetch_related("tags")`` resolves every tag
  pointing at every article in a single SELECT
  (``content_type = ct AND object_id IN (…)``), groups by
  ``object_id``, and stamps each article's manager cache. The
  ``GenericRelation`` manager's ``.all()`` reads from that cache
  before falling back to the live query — same contract as Django's
  prefetch + reverse-FK relation.
- **Async parity** via ``_aprefetch_generic_relation``, dispatched
  alongside the existing async prefetch coroutines through
  :func:`asyncio.gather`.
- **Custom ``Prefetch(queryset=…)`` is honoured** — the user-supplied
  queryset's filters / ordering / select_related survive; the
  ``content_type`` + ``object_id__in`` predicates are AND-ed onto it.

### Added — `only()` / `defer()` compose with `select_related`

- **Dotted paths now restrict the related projection.**
  ``Author.objects.select_related("publisher").only("name", "publisher__name")``
  emits a single LEFT OUTER JOIN that pulls only ``"authors"."id"``,
  ``"authors"."name"``, ``"publishers"."id"``, and
  ``"publishers"."name"`` — previously the projection-restriction
  short-circuited the JOIN entirely, silently degrading the query
  to a per-row N+1 on the related side.
- **PK is always implicit.** Even when the user lists only non-PK
  related columns, the related model's PK column is added to the
  projection so the hydrated instance keeps a valid identity.
- ``defer("publisher__bio")`` is the inverse: keeps every related
  column except the named one(s).
- **Bare names retain legacy semantics.** ``only("name")`` (no
  dotted path) restricts only the parent model; the related side
  loads in full. The parent and related restrictions live in
  separate state buckets so combining them
  (``only("name").defer("publisher__bio")``) works.

### Added — Pydantic Create / Update schema helpers

- **``create_schema_for(model)``** — drops auto-incrementing PKs
  and ``GeneratedField`` columns automatically (server-controlled),
  keeps required fields required, propagates real defaults. Plain
  ``BaseModel`` subclass suitable as a FastAPI request body. Default
  class name ``f"{Model.__name__}Create"``.
- **``update_schema_for(model)``** — every remaining column becomes
  ``T | None`` with default ``None`` (PATCH semantics). Built via
  :func:`pydantic.create_model` directly so the field-default
  propagation can be neutralised — a column with ``default=False``
  must not advertise that default to the client when partial-update
  semantics say "no change". Constraint translation (``max_length``,
  ``ge=0``, ``Literal[…]`` for ``choices``, …) still applies — PATCH
  bodies aren't a free pass on validation.
- Both helpers accept ``name=``, ``exclude=`` (extends the auto-PK
  drop), and ``base=`` (custom ``BaseModel`` ancestor).

### Fixed — `select_related` + `only()` was silently a no-op

- The SQL builder used to short-circuit the ``select_related`` JOIN
  whenever ``only()`` / ``defer()`` was active, on the (incorrect)
  assumption that the restricted projection would conflict with the
  aliased SR columns. The aliased columns (``"_sr_<path>_<col>"``)
  are namespaced by construction, so emitting both is safe. The
  short-circuit is gone; SR JOINs now run alongside any projection
  restriction.
- ``ORDER BY`` now qualifies the parent column with the table name
  whenever any JOIN is in flight (WHERE-derived **or**
  select_related). The previous "qualify only when ``self.joins`` is
  truthy" rule missed the SR case and produced ``ambiguous column
  name: id`` once a parent ``"id"`` and a related ``"id"`` both
  showed up unqualified.

## [2.3.1] - 2026-04-29

### Performance — `prefetch_related` on `GenericForeignKey`

- **N+1 query collapsed to 1 + K + 1.** Iterating a queryset of
  polymorphic-tagged rows used to do one ``model.objects.get(pk=oid)``
  per row inside the descriptor — for a list of N tags pointing at K
  distinct content types that's N round-trips. ``prefetch_related("target")``
  now groups instances by ``content_type_id``, bulk-fetches every
  referenced :class:`ContentType` in a single SELECT (warming the
  manager's ``(app_label, model)`` cache as it goes), and then issues
  one ``filter(pk__in=…)`` per content type. The descriptor's read path
  still hits the same cache slot, so existing user code lights up
  automatically the moment ``prefetch_related("target")`` is added.
- **Async parity.** ``_aprefetch_gfk`` runs the per-content-type bulk
  fetches concurrently via ``asyncio.gather``; K content types cost
  one round-trip's worth of latency, not K.
- **Mixed prefetches work.** ``prefetch_related("target", "content_type")``
  on the same queryset routes the GFK and the regular FK through their
  respective dispatcher branches in one call.
- **Misuse is rejected up front.** ``Prefetch("target", queryset=…)``
  and ``Prefetch("target", to_attr="…")`` both raise
  ``NotImplementedError`` with a hint pointing the user at
  per-concrete-relation prefetches — a single user-supplied queryset
  can't filter all targets of a heterogeneous GFK.
- **Tests** in ``tests/test_gfk_prefetch.py`` (22 cases) pin the
  query budget (1 + 1 + K), descriptor cache reuse (0 SELECTs after
  warm-up), correctness across two content types, dangling targets,
  empty / null cases, async parity, and the validation errors.

### Fixed — Manager type hint

- ``Manager.prefetch_related`` now declares ``*fields: str | Prefetch``
  instead of just ``str``. The runtime always accepted both, but
  ``ty`` rightly flagged ``Manager.objects.prefetch_related(Prefetch(…))``
  as a type error. No behaviour change.

## [2.3.0] - 2026-04-29

The 2.3 release sharpens the **FastAPI / Pydantic** integration so
field-level constraints declared on dorm models actually surface at the
API boundary (HTTP 422 + OpenAPI), fixes a **connection-leak class** that
was producing random ``ResourceWarning`` and intermittent CI hangs on
Python 3.14, and adds **178 new tests** that close several coverage gaps
across content types, the Pydantic layer, and the SQLite backend.

### Added — Pydantic constraint translation

- **`max_length` propagates to the schema.** ``CharField(max_length=N)``,
  ``EmailField`` (default 254), ``URLField`` (default 200), ``SlugField``
  (default 50), ``FileField``, and ``EnumField`` of strings now generate
  ``Annotated[str, Field(max_length=N)]`` — and by extension
  ``"maxLength": N`` in ``Schema.model_json_schema()``. Previously the
  constraint was enforced only at ``full_clean`` / DB time, so FastAPI
  accepted oversize strings and the user only saw the failure deep
  inside the request handler.
- **`DecimalField.max_digits` / `decimal_places`** translate to
  Pydantic's ``Field(max_digits=…, decimal_places=…)``.
- **`choices=[…]` becomes `Literal[…]`** in the annotation. Both
  the canonical ``[(value, label), …]`` shape and the flat ``[value, …]``
  shape are accepted. Members render as an ``"enum": […]`` array in the
  JSON Schema; non-members are rejected with the usual Pydantic error.
- **`PositiveIntegerField` / `PositiveSmallIntegerField`** carry a
  ``ge=0`` / ``"minimum": 0`` constraint instead of just ``int``.
- **`EmailField` / `URLField` advertise their format hint** —
  ``"format": "email"`` / ``"format": "uri"`` — so OpenAPI clients
  render the right input affordance and downstream code generators
  pick the right type, *without* dragging in the optional
  ``email-validator`` dependency.
- **Built-in validators are translated.** ``MinValueValidator`` →
  ``ge=N``, ``MaxValueValidator`` → ``le=N``, ``MinLengthValidator`` →
  ``min_length=N``, ``MaxLengthValidator`` → ``max_length=N``,
  ``RegexValidator`` → ``pattern=str``. When a validator and a native
  field constraint disagree the *strictest* wins (e.g.
  ``CharField(max_length=20, validators=[MaxLengthValidator(5)])``
  effectively bounds the schema at 5).

### Fixed — Default value propagation

- **Field defaults are now exposed in the schema.** ``BooleanField(default=False)``
  / ``IntegerField(default=3)`` / ``CharField(default="anon")`` previously
  produced ``T | None`` with default ``None`` — meaning a request body
  that omitted the field arrived at the model as ``None`` instead of
  the field's real default. The schema now exposes the actual default
  (or ``default_factory`` for callable defaults), and the annotation
  stays the bare type ``T`` (no spurious ``| None``). Nullable columns
  without a default still render as ``T | None`` with default ``None``.

### Fixed — Connection leaks under `pytest -n 4` / Python 3.14

- **Async SQLite connections are now closed deterministically across
  event-loop boundaries.** ``SQLiteAsyncDatabaseWrapper._check_loop``
  used to drop the held aiosqlite connection by setting
  ``self._conn = None`` whenever a new event loop was detected; the
  underlying ``sqlite3.Connection`` was finalised by the GC later as
  ``ResourceWarning: unclosed database`` and an aiosqlite worker
  thread stayed parked on its queue. The new ``_force_close_sync``
  helper drives ``aiosqlite.Connection.stop()`` and joins the worker
  thread (5 s bounded) so the handle is gone before we move on. New
  ``force_close_sync()`` method on the async wrappers, called by
  ``reset_connections`` and the atexit hook, plugs the same hole at
  test-teardown / process-exit time.
- **Sync SQLite wrapper now closes connections from every thread.**
  ``SQLiteDatabaseWrapper`` mirrors its ``threading.local`` cache in
  a thread-id-keyed dict, so ``close()`` releases the handles opened
  in worker threads as well as the calling thread's. Previously,
  cross-thread connections leaked silently.
- **Validation runs *before* `sqlite3.connect`** in both sync and
  async ``_new_connection``. An ``ImproperlyConfigured`` raised on a
  bad ``OPTIONS["journal_mode"]`` no longer leaks the just-opened
  handle to the GC.
- **Test-suite teardown closes async wrappers explicitly** in the
  session-scoped fixture, eliminating the warnings pytest's
  ``unraisableexception`` plugin used to flush near the end of each
  run.

### Added — Coverage uplift (178 new tests)

- **`dorm.contrib.contenttypes` 75% → 96%.** New tests cover
  ``GenericForeignKey.aget`` (async resolution, cache hits, deleted
  target, unset descriptor), ``_GenericRelatedManager`` extras
  (``exclude``, ``exists``, ``first``, ``add``, ``acreate``,
  ``_act_filter`` round-trip), descriptor-on-class access for both
  ``GenericForeignKey`` and ``GenericRelation``,
  ``GenericRelation._resolve_related`` happy / sad / class-target
  paths, and explicit ``ContentType.objects.clear_cache``
  invalidation.
- **`dorm.contrib.pydantic` constraint-edge cases.** Validator
  merging (``MinLengthValidator`` + ``MaxLengthValidator``, multiple
  ``MaxValueValidator``s), recursive types (``ArrayField`` element
  type, ``GeneratedField`` delegating to ``output_field``),
  ``_coerce_field_file_to_str`` fallback for objects without
  ``.name``, ``Meta.nested`` with FK / M2M / unknown-field error,
  ``Meta.fields`` + ``Meta.exclude`` mutual exclusion, missing
  ``Meta.model``, and explicit user annotations winning over the
  auto-derived ones.
- **`dorm/db/backends/sqlite.py` 81% → 84%.** ``_is_single_statement``
  edge cases (empty input, semicolons inside string literals /
  double-quoted identifiers), ``_validate_journal_mode`` error
  paths, sync wrapper auto-reconnect on a stale handle, multi-thread
  ``close()``, idempotent close, ``set_autocommit`` flipping
  ``isolation_level`` on the live connection, ``get_table_columns`` /
  ``pool_stats`` parity with the async wrapper, and a regression
  guard that asserts an invalid ``journal_mode`` doesn't emit a
  ``ResourceWarning``.
- **`dorm/db/connection.py` 85% → 88%.** ``DATABASE_ROUTERS``
  branches (router missing ``db_for_read``, raising router falling
  through, falsy alias ignored), unknown-alias error, ``health_check``
  / ``pool_stats`` happy + uninitialised paths, idempotent
  ``force_close_sync``, ``close_all_async`` clearing the registry.

### Added — Tier 3 Django-parity features

- **`CompositePrimaryKey(*field_names)`** — declare a primary key
  spanning multiple existing fields. The migration writer emits
  ``PRIMARY KEY (col1, col2, …)`` (and strips the per-column
  ``PRIMARY KEY``); ``Model.pk`` returns / accepts a tuple;
  ``filter(pk=(a, b))`` decomposes into per-component WHERE clauses
  at the queryset boundary; ``save`` / ``delete`` use the multi-
  column predicate. Documented limitations: no auto-increment on
  components, can't be the *target* of a regular ``ForeignKey``,
  ``filter(pk__in=[...])`` is unsupported (use ``Q`` with explicit
  per-field clauses).

- **`dorm.contrib.contenttypes`** — Django-style polymorphic FKs.
  Adds ``ContentType`` (one row per registered model, keyed by
  ``(app_label, model)``), ``GenericForeignKey`` (virtual field
  composing ``content_type`` + ``object_id`` into a single
  descriptor), and ``GenericRelation`` (reverse accessor on the
  target side). ``ContentTypeManager.get_for_model`` /
  ``aget_for_model`` memoises lookups per process; descriptor
  reads cache the resolved instance per row. Includes async paths
  (``aget`` on the GFK descriptor, ``acreate`` on the relation
  manager). Tests cover create / cached lookup / round-trip /
  polymorphic targets / dangling object-id / reverse manager /
  isolation between instances on both SQLite and PostgreSQL.

### Added — Transaction lifecycle hooks
- **`transaction.on_rollback(callback, using="default")`** and the
  matching async **`transaction.aon_rollback`**. The mirror of
  ``on_commit`` for code that needs to undo non-transactional side
  effects when the surrounding ``atomic()`` rolls back. Examples:
  deleting a file just written to a storage backend, removing a key
  from a cache, sending a "previous notification was reverted"
  webhook. Outside an active transaction the callback is dropped
  (mirror of ``on_commit``'s "fire immediately" path); inside nested
  ``atomic()`` blocks, callbacks fire when *their* block rolls back —
  savepoint rollbacks fire only inner callbacks.

### Fixed — FileField + atomic() no longer leaks orphan files
- **A file written inside ``atomic()`` is now cleaned up
  automatically when the transaction rolls back.** Before this
  release, ``FileField.pre_save`` called ``storage.save`` *during*
  the transaction; if the surrounding block then raised, the bytes
  stayed on disk / S3 with no row referencing them. The save path
  now registers an ``on_rollback`` cleanup that calls
  ``storage.delete(name)`` on the just-written file, so a
  ``RuntimeError`` mid-block leaves the storage as it was before
  the block. Savepoint rollbacks clean up only the files written
  inside that savepoint; files written in the outer block (which
  still commits) are preserved. Outside any ``atomic()`` block, no
  cleanup is registered — there's nothing to undo.

### Fixed — Behaviour bugs (observable changes; review on upgrade)
- **`Q()` is now a tautology, not a no-op.** ``Q() | Q(age=2)``
  now correctly matches *every* row (``TRUE OR (age=2) ⇒ TRUE``);
  previously the empty ``Q()`` was silently dropped from the OR
  and the query collapsed to ``Q(age=2)``. AND with empty Q
  remains a no-op (``TRUE AND X ⇒ X``), unchanged. Code that
  relied on the buggy behaviour to filter rows via ``Q() | …``
  will now return more rows — review your filters.
- **`filter(col=F("other_col"))` now works for the comparison
  lookups** (``exact``, ``gt``, ``gte``, ``lt``, ``lte``).
  Previously the ``F`` object was passed as a bound parameter and
  the cursor errored out with ``"type 'F' is not supported"``.
  Other lookups raise ``NotImplementedError`` with a clear hint
  pointing at ``annotate()``.
- **`order_by("fk_name")` now resolves to the underlying
  ``fk_name_id`` column** when ``fk_name`` is a ``ForeignKey`` on
  the model. Previously the SQL emitted ``ORDER BY "fk_name"``,
  which doesn't exist as a column — the query crashed on PG and
  silently misbehaved on SQLite.
- **`bulk_create([Model(pk=42, …)])` now honours the explicit pk.**
  The previous code excluded every ``AutoField`` column from the
  INSERT regardless of whether the caller had pre-assigned the pk,
  so rows ended up at fresh auto-generated ids instead of the
  requested ones. This regressed Django-style fixtures that pin
  primary keys.
- **`bulk_update(rows, [])` now raises ``ValueError``** instead of
  emitting malformed ``UPDATE … WHERE …`` (no ``SET`` clause). Same
  fix on the async ``abulk_update`` path.

### Improved — `FileField(upload_to=callable)`
- **Migration writer round-trips module-level callables.** A
  ``FileField(upload_to=upload_owner_scoped)`` declared with a
  function defined at module scope now serialises to
  ``upload_to=upload_owner_scoped`` plus the matching
  ``from <module> import <fn>`` line in the migration's header — no
  more silent ``FIXME`` for the common dynamic-path pattern.
  Lambdas and nested functions still fall back to the FIXME marker
  (they have no stable importable name); the marker text now
  explains *why* and how to fix it.
- **Documentation surfaces dynamic-upload patterns.** The "Files"
  section in ``docs/models.md`` (EN + ES) gains a "Dynamic upload
  paths" subsection with three realistic examples (owner-scoped
  prefixes, route-by-extension, content-addressed) plus migration-
  round-trip rules and a path-safety note.
- **End-to-end coverage** in
  ``tests/test_filefield_callable_upload_to.py``: 28 tests across
  rendering, save/load, async parity, FK-aware paths, lambdas,
  collision handling under shared dynamic paths, and the writer's
  ability (or inability) to round-trip each callable shape.

## [2.2.0] - 2026-04-28

The 2.2 release adds **file storage** as a first-class concern of the
ORM (FieldField + pluggable backends), four field types that 2.1
left on the roadmap, async signal receivers, and JSON fixture
loading. Every feature ships against both SQLite and PostgreSQL; the
S3 backend is exercised end-to-end against MinIO in CI.

### Fixed — Migration writer
- **`makemigrations` now serialises every public field type.**
  ``_FIELD_IMPORTS`` and ``_serialize_field`` were stuck at the 2.0
  set, so a model with an ``EnumField`` produced ``EnumField()``
  with no enum class (silently broken migration), and
  ``DurationField`` / ``CITextField`` / ``ArrayField`` /
  ``GeneratedField`` / the range family / ``PositiveSmallIntegerField``
  / ``FileField`` were missing their import lines (``NameError`` at
  load). The writer now covers every type, recurses into
  ``ArrayField.base_field`` / ``GeneratedField.output_field``, and
  emits the user-side ``from <module> import <Enum>`` line for
  ``EnumField`` references. Round-trip tests in
  ``tests/test_migration_writer_new_fields.py`` execute every
  generated file to catch regressions.

### Added — Operational tooling
- **`dorm doctor` audits `STORAGES`.** Warns when the ``default``
  alias is missing, when ``FileSystemStorage.location`` is not a
  writable directory, when ``S3Storage`` is missing ``bucket_name``
  or has hardcoded ``access_key`` / ``secret_key`` (a near-universal
  prod red flag), or when ``endpoint_url`` uses plain HTTP for a
  non-local host. Adds a note when ``FileField`` is in use but
  ``STORAGES`` is unset (dorm falls back to ``./media``).
- **`dorm inspectdb` recognises 2.2's PG types.** ``INTERVAL`` →
  ``DurationField``, ``CITEXT`` → ``CITextField``, ``int4range`` /
  ``int8range`` / ``numrange`` / ``daterange`` / ``tstzrange`` →
  the matching ``RangeField`` subclass. Projects adopting dorm
  against a pre-existing schema get the right field classes
  instead of the ``TextField`` fallback.
- **`dorm init` settings template includes commented `STORAGES`
  blocks** for ``FileSystemStorage``, AWS S3, and S3-compatible
  services (MinIO / R2 / B2) — see ``cmd_init``.

### Changed — Public API
- **`Field.uses_class_descriptor`** (renamed from
  ``_uses_class_descriptor``) is now a documented opt-in for custom
  field subclasses that install themselves as class-level
  descriptors and need ``__set__`` to fire on assignment. The leading
  underscore signalled "private", but third-party fields with the
  same need (encryption, audit, lazy resolution) want the same hook —
  the rename promotes it to a stable extension point. ``FileField``
  is the canonical built-in user.

### Added — File storage
- **`FileField(upload_to=, storage=, max_length=)`** — pluggable file
  storage. The column is a ``VARCHAR(max_length)`` holding the
  storage-side name; the Python value is a ``FieldFile`` wrapper that
  delegates ``.url`` / ``.size`` / ``.open()`` / ``.delete()`` to the
  configured backend. ``upload_to`` accepts a static string, a
  ``strftime`` template, or a callable
  ``f(instance, filename) -> str``.
- **`dorm.storage` module**: ``Storage`` abstract base, default
  ``FileSystemStorage`` (local disk, default backend), ``File`` /
  ``ContentFile`` wrappers, ``FieldFile`` (descriptor result),
  ``get_storage(alias)`` registry and a ``default_storage`` proxy
  that re-resolves on every call. Storage methods come in sync + async
  pairs; async defaults wrap sync via ``asyncio.to_thread`` so backends
  with no native async client still work without thread blocking the
  event loop.
- **`STORAGES` setting** — multi-alias config that mirrors
  ``DATABASES``::

      STORAGES = {
          "default": {
              "BACKEND": "dorm.storage.FileSystemStorage",
              "OPTIONS": {"location": "/var/app/media",
                          "base_url": "/media/"},
          },
      }

  ``dorm.configure(STORAGES=...)`` invalidates the storage cache so
  the next ``get_storage()`` re-reads.
- **`dorm.contrib.storage.s3.S3Storage`** — AWS S3 backend gated
  behind the new ``s3`` extra (``pip install 'djanorm[s3]'``). Lazy
  ``boto3`` client init, presigned-URL support
  (``querystring_auth``), CDN / vanity-domain ``custom_domain``,
  configurable ``default_acl``, ``location`` prefix, opt-in
  ``file_overwrite`` for content-addressed layouts. Works with any
  S3-compatible service (MinIO, Cloudflare R2, Backblaze B2) via
  ``endpoint_url=``.
- **Path-traversal hardening** in ``FileSystemStorage``: every
  ``save`` / ``open`` / ``delete`` resolves ``name`` against an
  absolute ``location`` and refuses any path that escapes the root,
  even if the basename slipped through ``get_valid_name``.

### Added — Field types
- **`DurationField`** stores `datetime.timedelta`. Native ``INTERVAL``
  on PostgreSQL; on SQLite a process-wide ``sqlite3.register_adapter``
  encodes durations as integer microseconds in a ``BIGINT`` column so
  the same Python value round-trips on both backends.
- **`EnumField(enum_cls)`** stores an `enum.Enum` member. Column type
  is derived from the value type (string → ``VARCHAR``, int →
  ``INTEGER``); ``choices`` is auto-populated from the enum so admin /
  form layers see every member without restating them in ``Meta``.
- **`CITextField`** — case-insensitive text. Maps to PostgreSQL's
  ``CITEXT`` (extension required) and falls back to
  ``TEXT COLLATE NOCASE`` on SQLite.
- **Range fields** — ``IntegerRangeField``, ``BigIntegerRangeField``,
  ``DecimalRangeField``, ``DateRangeField``, ``DateTimeRangeField``.
  The Python value is ``dorm.Range(lower, upper, bounds="[)")``;
  SQLite raises ``NotImplementedError`` from ``db_type()`` so the
  limitation surfaces at migrate time, not at first query.

### Added — Async signals
- **`Signal.asend(...)`** — async dispatch entry point. Awaits
  coroutine receivers sequentially (in the order they were connected)
  and calls sync receivers directly. ``Model.asave`` /
  ``Model.adelete`` now route through ``asend`` so an
  ``async def post_save`` receiver fires from the async path.
- **Sync `Signal.send` skips coroutine receivers with a `WARNING`** on
  the ``dorm.signals`` logger instead of silently dropping them or
  deadlocking on ``asyncio.run``. Connect the same receiver and dorm
  picks the right dispatch automatically based on whether the caller
  used the sync or async ORM path.

### Added — Fixtures CLI
- **`dorm dumpdata`** — serialise model rows to JSON. Format mirrors
  Django's (`{model, pk, fields}` records); FKs as the target's PK,
  M2M as a list of related PKs. Custom envelopes preserve types JSON
  can't represent natively (decimals, UUIDs, datetimes, durations,
  ranges, bytes).
- **`dorm loaddata`** — load JSON fixtures back. Each file runs in a
  single ``atomic()`` block; M2M relations restore in a second phase
  after all parent rows land. ``save()`` and signals are bypassed for
  performance — use ``Model.save()`` when you do want pre-save hooks
  to fire.
- **`dorm.serialize`** module exposes the same operations
  programmatically: ``serialize``, ``dumps``, ``deserialize`` and
  ``load``.

### Changed
- **`SQLQuery._compile_leaf` now routes the bound value through the
  resolved field's `get_db_prep_value`** before reaching the cursor.
  Custom field types (``EnumField``, ``DurationField``,
  ``RangeField`` …) bind in their wire form rather than as opaque
  Python objects. ``__in`` lookups coerce element-by-element; lookups
  whose value is structural (``isnull``, ``range``, ``regex``) bypass
  the coercion as before.
- **`dorm sql --all` skips models whose fields have no SQL on the
  active backend** (typical case: a ``RangeField`` while introspecting
  against SQLite). The skip is reported on stderr; previously the
  whole dump aborted on the first incompatible model.

## [2.1.0] - 2026-04-27

The 2.1 release closes the biggest gap left in 2.0 for "production
reporting" workloads: querying. It also tightens the migration story
for tables large enough that an `ALTER TABLE` would page someone.

### Added — Querying

- **`Subquery()` and `Exists()`** with **`OuterRef("...")`** for
  correlated subqueries. ``filter(Exists(qs))`` and
  ``annotate(latest=Subquery(qs))`` work end-to-end; subqueries
  participate in the outer query's placeholder rewrite, so PostgreSQL
  prepared-statement caches stay efficient.

- **Window functions**: ``Window(expr, partition_by=, order_by=,
  frame=)`` plus seven canonical ranking / offset functions —
  ``RowNumber``, ``Rank``, ``DenseRank``, ``NTile``, ``Lag``,
  ``Lead``, ``FirstValue``, ``LastValue``. Ranking constructors
  refuse to compile without an ``order_by`` (the SQL would parse but
  return implementation-defined results — exactly the bug that ships
  to a dashboard).

- **Non-recursive CTEs**: ``QuerySet.with_cte(name=qs)`` emits the
  leading ``WITH name AS (sub)`` clause. Multiple CTEs can be chained;
  parameter binding flows through one rewrite pass.

- **New scalar functions**: ``Greatest`` / ``Least`` (vendor-aware —
  ``GREATEST``/``LEAST`` on PG, multi-arg ``MAX``/``MIN`` on SQLite),
  ``Round``, ``Trunc(unit=)``, ``Extract(unit=)``, ``Substr``,
  ``Replace``, ``StrIndex`` (``STRPOS`` on PG, ``INSTR`` on SQLite).
  Unit values are validated against an allowlist at queryset build
  time.

- **`QuerySet.cursor_paginate(after=, order_by=, page_size=)`** —
  keyset pagination that returns a ``CursorPage(items, next_cursor)``.
  Stable across writes, O(1) deep-page cost vs ``OFFSET``'s O(N).
  Async counterpart: ``acursor_paginate``.

- **PostgreSQL full-text search** (``dorm.search``):
  ``SearchVector(*fields, config=, weight=)``,
  ``SearchQuery(value, search_type=)`` (``plain`` / ``websearch`` /
  ``raw``), ``SearchRank(vector, query, cover_density=)``. New
  ``__search`` lookup wraps the canonical
  ``to_tsvector('english', col) @@ plainto_tsquery('english', %s)``
  pattern. SQLite is unsupported (use FTS5 virtual tables).

### Added — Schema

- **`CheckConstraint` and `UniqueConstraint`** (with optional
  ``condition=``) in ``Meta.constraints``. ``UniqueConstraint(fields=,
  condition=Q(...))`` becomes a *partial* unique index — the canonical
  "only one active row per user" pattern. The autodetector emits
  ``AddConstraint`` / ``RemoveConstraint`` migration ops.

- **`GeneratedField(expression=, output_field=, stored=True)`** for
  database-computed columns (PG ≥ 12, SQLite ≥ 3.31). Field assignment
  is rejected at runtime; the database is the source of truth. The
  expression grammar is allow-listed (alphanumerics, arithmetic,
  parens, dot, quotes, modulo) — anything more exotic should be issued
  via a ``RunSQL`` migration.

- **Index extensions**:
  - ``method=`` parameter accepting ``"btree"`` (default), ``"hash"``,
    ``"gin"``, ``"gist"``, ``"brin"``, ``"spgist"``, ``"bloom"``.
    SQLite silently uses B-tree.
  - ``condition=Q(...)`` for partial indexes
    (``CREATE INDEX ... WHERE pred``).
  - ``opclasses=[...]`` per-column operator classes (PostgreSQL).
  - Expression fields like ``"LOWER(email)"`` validated against a
    small allow-listed grammar.

### Added — Migration safety

- **`AddIndex(..., concurrently=True)` / `RemoveIndex(...,
  concurrently=True)`** emit ``CREATE INDEX CONCURRENTLY`` / ``DROP
  INDEX CONCURRENTLY`` on PostgreSQL — the canonical zero-downtime
  pattern for hot tables. SQLite ignores the flag.

- **`SetLockTimeout(ms=...)`** sets PostgreSQL's ``lock_timeout`` for
  the migration window so DDL on a hot table fails fast on contention
  instead of blocking writers indefinitely. SQLite is a no-op.

- **`ValidateConstraint(table=..., name=...)`** runs ``ALTER TABLE
  ... VALIDATE CONSTRAINT``. Pair with a ``RunSQL("ALTER TABLE ...
  ADD CONSTRAINT ... NOT VALID")`` to add foreign keys / CHECK
  constraints to a billion-row table without an
  ``AccessExclusiveLock``: the validation pass takes only
  ``ShareUpdateExclusive`` and runs concurrently with reads/writes.

### Added — Operations / tooling

- **`dorm inspectdb`** — reverse-engineer ``models.py`` from the
  connected database. Best-effort type recovery (constraints / indexes
  not introspected); pipe to a file and edit.

- **`dorm doctor`** — audit the running configuration for
  production-mode footguns (small ``MAX_POOL_SIZE``, missing
  ``sslmode`` on a remote PG host, FKs without an index, no retry
  configured for transient errors). Exits non-zero on any warning, so
  it doubles as a pre-deploy gate.

- **URL/DSN support in `DATABASES`**:
  ``DATABASES = {"default": "postgres://u:p@host:5432/db?sslmode=require"}``
  or ``{"default": {"URL": "postgres://...", "MAX_POOL_SIZE": 20}}``.
  ``parse_database_url(url)`` is also exported so the same parser
  powers ``DATABASE_URL=...`` env-var deployments. Well-known pool
  knobs (``MAX_POOL_SIZE``, ``POOL_TIMEOUT``, ``POOL_CHECK``,
  ``MAX_IDLE``, ``MAX_LIFETIME``, ``PREPARE_THRESHOLD``) are lifted
  to top-level keys; everything else lands in ``OPTIONS``.

### Changed

- **`QuerySet.alias()` / `annotate()` now thread `connection` to
  expression `as_sql()` calls.** Required for the new vendor-aware
  functions (``Greatest``, ``Least``, ``StrIndex``) to pick the right
  SQL idiom. ``Aggregate.as_sql`` now accepts ``**kwargs`` so legacy
  custom aggregates keep compiling.

- **`Index.__init__`** now validates each field name (or expression)
  at construction time. A leading ``-`` is still permitted as the
  Django-style descending-column hint and surfaces as ``DESC`` in the
  emitted SQL. Existing ``Index(fields=["col"])`` declarations are
  unchanged.

### Documentation

- **Bilingual docs (EN + ES)** updated with new sections covering
  every 2.1 feature, plus a new "Querying recipes" cookbook page that
  shows running totals, top-N-per-group, deltas, percentiles, and
  partial-unique patterns.


### Fixed (carried in from 2.0.x development)
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

### Added (carried in from 2.0.x development)
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

### Security (carried in from 2.0.x development)
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

### Changed (carried in from 2.0.x development)
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
