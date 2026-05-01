# Changelog

All notable changes to djanorm are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [2.4.1] - 2026-05-01

Bug-hunt patch release. Fifteen latent correctness / dataloss /
async-parity / migration-safety issues surfaced by a multi-agent
audit, all closed with regression tests in
``tests/test_bug_hunt_v2_5.py``. No public API changes.

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
  PostgreSQL-only skipped without Docker) covering every fix
  above. Split sync / async where applicable; runs on both
  SQLite and PostgreSQL.

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
