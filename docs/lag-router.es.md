# Lag-aware read routing

`dorm.contrib.lag_router.LagAwareReadRouter` (3.4+) consulta
`pg_last_xact_replay_timestamp()` y desvía reads al primary
cuando una réplica está demasiado lagged. Diferencia clave: dorm
no se limita a "round-robin entre réplicas" — la salud de cada
réplica se evalúa antes de cada batch de reads.

PostgreSQL-only.

## El problema

```
[primary] --- WAL stream ---> [replica_1]   lag: 0.4s ✓
                              [replica_2]   lag: 12.0s ✗
```

Si tu app envía reads round-robin sin chequeo, `replica_2` te
devolverá datos *atrás de 12 segundos*. Para queries de saldo,
estado de pago, etc., eso es un bug de correctitud.

## Configuración

```python
from dorm.contrib.lag_router import LagAwareReadRouter

DATABASES = {
    "primary": {...},
    "replica_1": {...},
    "replica_2": {...},
}

DATABASE_ROUTERS = [
    LagAwareReadRouter(
        primary="primary",
        replicas=["replica_1", "replica_2"],
        max_lag_seconds=2.0,    # umbral de desvío
        cache_seconds=5.0,      # cuánto cacheamos la lectura de lag
    ),
]
```

`max_lag_seconds=2.0` significa: réplicas con lag > 2s se evitan;
los reads se mandan al primary directamente. Cache de 5s
amortiza el coste del check (si comprobamos lag por cada query
saturamos PG con `pg_last_xact_replay_timestamp()`).

## Comportamiento

```python
reads = Order.objects.filter(...)
# 1. Router pregunta a cada réplica su lag (si cache expirada).
# 2. Si replica_1 tiene 0.4s lag → ruta ahí.
# 3. Si todas están >max_lag → log WARNING + ruta primary.

writes = Order.objects.create(...)
# Siempre primary.
```

## Inspección

```python
router = DATABASE_ROUTERS[0]
print(router.snapshot())
# {
#   "replica_1": {"lag_seconds": 0.4, "healthy": True, "checked_at": 12345.6},
#   "replica_2": {"lag_seconds": 12.0, "healthy": False, "checked_at": 12345.6},
# }
```

Útil para Prometheus exporters / dashboards.

## Caveats

- **`pg_last_xact_replay_timestamp()` puede ser NULL** en réplicas
  ociosas (ningún replay pendiente). El router lo trata como
  "0s lag" — totalmente caught up por definición.
- **Consultas vía `using("replica_1")` saltean el router** —
  fuerza el alias literal. Útil para overrides puntuales.
- **El cache es per-proceso**: cada worker mantiene su propio
  estado. Para coordinación cross-worker, monta encima Redis.
- **Failover de primary**: cuando el primary cambia, el router
  no se entera — usa una capa adicional (HAProxy, Patroni) para
  failover a nivel infra.

## Receta: con FastAPI

```python
from dorm.contrib.lag_router import LagAwareReadRouter

DATABASE_ROUTERS = [
    LagAwareReadRouter(
        primary="primary",
        replicas=["replica_eu", "replica_us"],
        max_lag_seconds=2.0,
    ),
]

@app.get("/orders/{pk}")
async def get_order(pk: int):
    order = await Order.objects.aget(pk=pk)   # auto-routed por router
    return order

# Caso especial: justo después de un write, lee del primary
@app.post("/orders")
async def create_order(body):
    order = await Order.objects.using("primary").acreate(...)
    # ... más reads forzando primary para read-after-write:
    fresh = await Order.objects.using("primary").aget(pk=order.pk)
    return fresh
```

## Versus alternativas

| Patrón | Cuándo |
|---|---|
| **Lag-aware router** (este) | Read-replica clásico con safety net |
| Read replicas plain (sin lag check) | Si tolerancia stale es alta (analytics) |
| Sticky read-after-write window (3.0+) | Single primary, evitar replica para reads recientes |
| `using("primary")` manual | Override puntual por endpoint |

## Más

- [Helpers](helpers.md#lag-aware-read-router)
- [Producción: réplicas](production.md)
- API: `dorm.contrib.lag_router`
