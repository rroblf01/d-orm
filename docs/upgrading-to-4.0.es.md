# Subiendo de 3.3 a 4.0

**TL;DR**: cero cambios obligatorios. Todo lo nuevo es opt-in.
Salta directamente al apartado [Quiero usar la feature X](#quiero-usar-la-feature-x).

## ¿Hay breaking changes?

**No.** Tu código de 3.3 sigue compilando, ejecutando y pasando
tests sin tocar una línea. Verificación que ejecutamos en CI:

```python
# Smoke test 3.3 → 4.0
class A(dorm.Model):
    name = dorm.CharField(max_length=10)

A.objects.create(name="x")
list(A.objects.filter(name__icontains="x"))
A.objects.bulk_create([A(name=f"a{i}") for i in range(10)])
```

Único cambio visible: el número de versión salta de 3.3.0 a 4.0.0
(saltamos 3.4 — todo lo planificado para 3.4 viaja en este
release junto con 7 features adicionales).

## Pasos de upgrade

### 1. Actualiza el paquete

```bash
pip install --upgrade djanorm
# o
uv add 'djanorm>=4.0,<5.0'
```

### 2. (Opcional) Instala extras nuevos

```bash
# Backend DuckDB para analítica embarcada
pip install 'djanorm[duckdb]'

# Sibling packages (dev tooling)
pip install pytest-djanorm djanorm-mypy
```

### 3. (Opcional) Activa features 4.0

Cada una bajo demanda — sigue las recetas en [Novedades 4.0](v4_0.md).

### 4. Re-ejecuta tu suite

```bash
pytest
ruff check
mypy        # con djanorm-mypy plugin si lo añadiste
```

Si algo falla, abre issue — no debería romperse nada.

## Quiero usar la feature X

| Quiero... | Lee |
|---|---|
| Ingestar millones de filas rápido | [Bulk COPY](bulk-copy.md) |
| Migración zero-downtime en tabla grande | [Online migrations](online-migrations.md) |
| Detectar drift schema en CI | `dorm diff` ([CLI](cli.md#dorm-diff-40)) |
| Multi-tenancy a nivel fila | [Row tenancy](tenants-row.md) |
| Árboles / categorías / hilos comentarios | [Recursive CTE](recursive-cte.md) |
| Backend OLAP embarcado | [DuckDB](duckdb.md) |
| Pub/sub PG sin broker | [LISTEN/NOTIFY](listen-notify.md) |
| Outbox pattern para microservicios | [Outbox](outbox.md) |
| Sharding horizontal | [Sharding](sharding.md) |
| Idempotency keys (Stripe-style) | [Idempotency](idempotency.md) |
| Circuit breaker | [Circuit breaker](circuit-breaker.md) |
| Read replicas con lag check | [Lag router](lag-router.md) |
| Streaming exports JSONL/CSV | [Helpers](helpers.md#streaming-primitives) |
| Query budget (HTTP SLA) | [Helpers](helpers.md#query-budget) |
| Geometrías / GIS | [GIS](gis.md) |
| HStore / ENUM nativo PG | [v4.0](v4_0.md#7-hstorefield-pg-enum-nativo) |
| Search full-text + trigram | [v4.0](v4_0.md#1-full-text-search-ampliado) |
| OTel traces enriquecidos | [v4.0](v4_0.md#3-opentelemetry-enriquecido) |
| Mypy validar `filter()` kwargs | [Sibling packages](sibling-packages.md) |
| Pytest fixtures `transactional_db` | [Sibling packages](sibling-packages.md) |

## Decisiones de diseño 4.0

Algunas cosas que NO añadimos deliberadamente:

- **Sin `dorm.contrib.fastapi`** — los helpers framework-agnósticos
  cubren todo lo necesario. Atar el wheel a FastAPI sería
  innecesario: 99% del público objetivo lo usa, pero 1% no.
  [Discusión](sibling-packages.md).
- **Sin `forms`** — el target API-first va a Pydantic. Si vienes
  de Django, [migration-from-django](migration-from-django.md)
  tiene la equivalencia.
- **Sin admin built-in** — `sqladmin` o tu dashboard custom.
  `dorm export-json-schema` te da el input para tooling externo.
- **mypy + pytest plugins en paquetes hermanos** — wheel principal
  no arrastra deps dev-only. [Rationale](sibling-packages.md).

## Versionado a partir de 4.0

- `djanorm 4.x` — minor bumps cada ~2-3 meses con features
  opt-in. Sin breaking.
- `djanorm-mypy` y `pytest-djanorm` — versionado independiente.
  Cada uno declara `djanorm>=4.0,<5.0` para compatibilidad cross.
- `djanorm 5.0` — algún día. Telegrafiaríamos rompimientos con
  warnings durante la 4.x antes.

## Migración inversa (rollback)

Si necesitas volver a 3.3:

```bash
pip install 'djanorm==3.3.0'
```

Tu código sigue funcionando si **no** usaste features 4.0
(`dorm.tree`, `dorm.contrib.tenants_row`, `AddFieldOnline`,
`HStoreField`, `EnumField(native=True)`, DuckDB, etc.).

Si tu BD tiene migraciones que aplican `AddFieldOnline` o
`CreatePGEnum`, esas migraciones no funcionan en 3.3 — saca un
`dump` de la BD antes y restaura en 3.3 si es necesario.

## Más

- [What's new in 4.0](v4_0.md) — lista completa de features
- [CHANGELOG.md](https://github.com/rroblf01/d-orm/blob/main/CHANGELOG.md) — release notes detallados
- [Issues GitHub](https://github.com/rroblf01/d-orm/issues) —
  reporta cualquier regresión 3.3 → 4.0
