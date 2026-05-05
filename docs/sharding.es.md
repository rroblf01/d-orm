# Sharding por hash

Cuando un dataset crece más allá de lo que cabe en una sola BD,
el patrón es **sharding horizontal**: partir filas entre N
servidores físicos por una *shard key* (típicamente tenant_id,
user_id, org_id).

`dorm.contrib.sharding` (3.4+).

## Cuándo usarlo

- Tabla principal pasó del TB; vertical scaling agotado.
- Tenants distribuidos geográficamente (US-east, EU-west).
- Carga write-heavy que satura un único primary.

## Cuándo NO

- <100GB por tabla y <5000 QPS — vertical scaling es mucho más
  simple.
- Si tus queries hacen JOINs cross-shard frecuentes — el
  sharding rompe esos. Replanteate el modelo de datos primero.
- Sin separación clara por una clave (queries todo-vs-todo
  contra cualquier subset).

## API

```python
from dorm.contrib.sharding import (
    HashShardRouter, with_shard_key, shard_for, for_each_shard,
)

# settings.py
DATABASES = {
    "default": {...},
    "shard_0": {...},
    "shard_1": {...},
    "shard_2": {...},
    "shard_3": {...},
}
DATABASE_ROUTERS = [
    HashShardRouter(num_shards=4, shard_models={Order, Customer}),
]
```

```python
# Request handler
from dorm.contrib.sharding import with_shard_key

@app.post("/orders")
async def create_order(request, body):
    with with_shard_key(request.user.tenant_id):
        order = await Order.objects.acreate(...)   # ruteado a shard_N
    return order
```

## Hash determinista

`shard_for(key, num_shards)` usa `hashlib.blake2b` con salt
configurable, **NO** Python's built-in `hash()` (que es
randomizado per-proceso desde Python 3.3 — usaría una asignación
de shards distinta en cada worker).

```python
from dorm.contrib.sharding import shard_for

assert shard_for("user-42", 4) == shard_for("user-42", 4)   # determinista
# Algunos llamadores prefieren su propio salt para seguridad:
shard_for("user-42", 4, salt=b"mi-salt-secreta")
```

## `for_each_shard` — fan-out

Para queries globales (count total, batch jobs por shard):

```python
from dorm.contrib.sharding import for_each_shard

results = for_each_shard(
    lambda alias: Order.objects.using(alias).count(),
    num_shards=4,
)
# {"shard_0": 1234, "shard_1": 1209, ...}

total = sum(results.values())
```

Secuencial; envuelve en `asyncio.gather` o threads para
paralelismo si es necesario.

## Composer con multi-tenancy fila

`HashShardRouter` + `TenantModel` componen elegantemente — la
shard key suele **ser** el tenant id:

```python
with current_tenant(tenant_id), with_shard_key(tenant_id):
    Note.objects.create(title="hi")
    # → tenant_id auto-fill + ruteo al shard correcto
```

## Sin shard key activa

Si tu modelo es sharded y no hay `with_shard_key()` activo:

```
RuntimeError: HashShardRouter: no active shard key for sharded model 'Order'
```

Por diseño. Un fallback silencioso a `default` repartiría rows
inconsistentemente entre shards.

## Rebalancing (shard splits)

dorm **no** rebalancea automáticamente. Si pasas de 4 → 8 shards:

1. Crea los nuevos shards (vacíos).
2. Cambia `num_shards=8` en producción — nuevos rows van con
   distribución nueva.
3. Por cada shard viejo, migra rows a su nuevo destino:
   ```python
   for row in OldShard.objects.using("shard_0").all():
       new_alias = shard_for(row.tenant_id, 8)
       row.save(using=new_alias)
       row.delete(using="shard_0")
   ```
4. Pausa o no del tráfico durante la migración: tu decisión
   ops/négocio.

Para evitar este dolor, **hash consistente** (consistent hashing
ring) en lugar de modulo. dorm no lo implementa de fábrica;
considera Citus o Vitess si lo necesitas.

## Pitfalls

- **JOINs cross-shard imposibles** — cada shard es una BD
  independiente. Modelo data antes de shardar.
- **`allow_relation` rechaza FKs cross-shard**: el router devuelve
  `False` cuando obj1 / obj2 viven en aliases distintos. Atrapas
  bugs en código antes de runtime.
- **Migraciones**: `dorm migrate` aplica solo en `default` por
  defecto. Para correr en cada shard:
  ```bash
  for alias in shard_0 shard_1 shard_2 shard_3; do
    dorm migrate --database $alias
  done
  ```

## Más

- [Helpers](helpers.md#hash-based-horizontal-sharding)
- [Multi-tenancy fila](tenants-row.md) — combinación natural
- API: `dorm.contrib.sharding`
