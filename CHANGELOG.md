# Changelog

All notable changes to djanorm are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
