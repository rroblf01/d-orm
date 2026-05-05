# Upgrading from 3.3 to 4.0

**TL;DR**: zero mandatory changes. Everything new is opt-in. Skip
straight to [I want feature X](#i-want-feature-x).

## Are there breaking changes?

**No.** Your 3.3 code keeps compiling, running and passing tests
without touching a line. CI smoke test we ship:

```python
# 3.3 → 4.0 smoke test
class A(dorm.Model):
    name = dorm.CharField(max_length=10)

A.objects.create(name="x")
list(A.objects.filter(name__icontains="x"))
A.objects.bulk_create([A(name=f"a{i}") for i in range(10)])
```

Only visible change: the version jumps 3.3.0 → 4.0.0 (we skip
3.4 — everything planned for 3.4 ships in this release together
with 7 additional features).

## Upgrade steps

### 1. Update the package

```bash
pip install --upgrade djanorm
# or
uv add 'djanorm>=4.0,<5.0'
```

### 2. (Optional) Install new extras

```bash
# DuckDB backend for embedded analytics
pip install 'djanorm[duckdb]'

# Sibling packages (dev tooling)
pip install pytest-djanorm djanorm-mypy
```

### 3. (Optional) Adopt 4.0 features

Each on demand — follow the recipes in [What's new in 4.0](v4_0.md).

### 4. Re-run your suite

```bash
pytest
ruff check
mypy        # with djanorm-mypy plugin if you added it
```

If something breaks, open an issue — nothing should regress.

## I want feature X

| I want… | Read |
|---|---|
| Ingest millions of rows fast | [Bulk COPY](bulk-copy.md) |
| Zero-downtime migration on a big table | [Online migrations](online-migrations.md) |
| Detect schema drift in CI | `dorm diff` ([CLI](cli.md#dorm-diff-40)) |
| Row-level multi-tenancy | [Row tenancy](tenants-row.md) |
| Trees / categories / comment threads | [Recursive CTE](recursive-cte.md) |
| Embedded OLAP backend | [DuckDB](duckdb.md) |
| PG pub/sub without a broker | [LISTEN/NOTIFY](listen-notify.md) |
| Outbox pattern for microservices | [Outbox](outbox.md) |
| Horizontal sharding | [Sharding](sharding.md) |
| Idempotency keys (Stripe-style) | [Idempotency](idempotency.md) |
| Circuit breaker | [Circuit breaker](circuit-breaker.md) |
| Read replicas with lag check | [Lag router](lag-router.md) |
| Streaming JSONL/CSV exports | [Helpers](helpers.md#streaming-primitives) |
| Query budget (HTTP SLA) | [Helpers](helpers.md#query-budget) |
| Geometries / GIS | [GIS](gis.md) |
| HStore / native PG ENUM | [v4.0](v4_0.md#7-hstorefield-native-pg-enum) |
| Full-text search + trigram | [v4.0](v4_0.md#1-expanded-full-text-search) |
| Enriched OTel traces | [v4.0](v4_0.md#3-enriched-opentelemetry-instrumentation) |
| mypy validating `filter()` kwargs | [Sibling packages](sibling-packages.md) |
| pytest fixtures `transactional_db` | [Sibling packages](sibling-packages.md) |

## 4.0 design decisions

Things we deliberately did NOT add:

- **No `dorm.contrib.fastapi`** — the framework-agnostic helpers
  cover everything. Coupling the wheel to FastAPI would be
  unnecessary: 99% of the target audience uses it, but the 1%
  shouldn't pay. [Discussion](sibling-packages.md).
- **No `forms`** — the API-first target uses Pydantic. Coming
  from Django, [migration-from-django](migration-from-django.md)
  has the equivalents.
- **No built-in admin** — `sqladmin` or your custom dashboard.
  `dorm export-json-schema` provides the input for external
  tooling.
- **mypy + pytest plugins in sibling packages** — the main wheel
  doesn't drag in dev-only deps. [Rationale](sibling-packages.md).

## Versioning from 4.0 onward

- `djanorm 4.x` — minor bumps every ~2-3 months with opt-in
  features. No breaking changes.
- `djanorm-mypy` and `pytest-djanorm` — independently versioned.
  Each declares `djanorm>=4.0,<5.0` for cross compatibility.
- `djanorm 5.0` — someday. We'd telegraph break-points with
  deprecation warnings during the 4.x line first.

## Rollback to 3.3

If you need to revert:

```bash
pip install 'djanorm==3.3.0'
```

Your code keeps working if you did **not** use 4.0 features
(`dorm.tree`, `dorm.contrib.tenants_row`, `AddFieldOnline`,
`HStoreField`, `EnumField(native=True)`, DuckDB, etc.).

If your DB has migrations applying `AddFieldOnline` or
`CreatePGEnum`, those migrations won't run on 3.3 — back up the
DB before and restore on 3.3 if necessary.

## More

- [What's new in 4.0](v4_0.md) — full feature list
- [CHANGELOG.md](https://github.com/rroblf01/d-orm/blob/main/CHANGELOG.md) — detailed release notes
- [GitHub issues](https://github.com/rroblf01/d-orm/issues) —
  report any 3.3 → 4.0 regression
