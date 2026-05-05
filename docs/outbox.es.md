# Outbox pattern

Resuelve el problema de la **doble escritura**: cuando una acción
necesita actualizar la BD *y* publicar un evento, hacer ambas
directamente abre una ventana donde la BD commitea pero el broker
falla. Inconsistencia.

El fix: escribe el evento en una tabla `outbox` **dentro de la
misma transacción** que la escritura de negocio. Un worker
separado dren la outbox y publica al broker. Los dos commitean
juntos atómicamente.

`dorm.contrib.outbox` (3.4+). Backend-agnóstico (PG, MySQL,
SQLite, libsql, DuckDB).

## API

```python
from dorm.contrib.outbox import OutboxEvent, OutboxRelay, record_event
from dorm import transaction

# 1. Define tu tabla outbox subclasando el abstract base
class Outbox(OutboxEvent):
    class Meta:
        db_table = "outbox"

# 2. En el handler — record + business write en la misma tx
with transaction.atomic():
    order = Order.objects.create(...)
    record_event(Outbox, "order.created", {"order_id": order.id})

# 3. Worker independiente — drena la outbox
def publish_to_kafka(row):
    kafka_client.send("orders", row.payload)
    return True       # éxito → marca published

relay = OutboxRelay(Outbox, batch_size=100)
relay.run(handler=publish_to_kafka)    # blocks; SIGTERM para parar
```

## Modelo `OutboxEvent`

Columnas built-in:

| Columna | Tipo | Para qué |
|---|---|---|
| `id` | UUID | PK |
| `event_type` | CharField(128) | Tipo de evento (`order.created`) |
| `payload` | JSONField | Datos arbitrarios |
| `status` | CharField(16) | `pending` / `published` / `dead` |
| `attempts` | IntegerField | Contador retry |
| `last_error` | TextField | Último mensaje de error |
| `created_at` | DateTimeField | Timestamp creación |
| `published_at` | DateTimeField | Timestamp publicación |

Override en tu subclase si necesitas más columnas o tipos
distintos.

## `OutboxRelay`

```python
relay = OutboxRelay(
    Outbox,
    batch_size=100,         # filas por batch
    poll_interval_s=1.0,    # segundos entre batches vacíos
    max_attempts=5,         # antes de marcar 'dead'
    using="default",
)
```

### Concurrencia: `SKIP LOCKED`

En PostgreSQL, `OutboxRelay` usa
`SELECT … FOR UPDATE SKIP LOCKED` para que **N relays paralelos
toman filas disjuntas**. Sin coordinación externa.

En backends sin SKIP LOCKED (SQLite, MySQL < 8) cae a SELECT
plano — multiple relays pueden duplicar trabajo. Haz el handler
**idempotente** (la receta correcta de todos modos).

### Dead letter

Filas que fallan `max_attempts` veces se marcan `status='dead'`.
El relay las salta — bajan a inspección manual.

```python
deadletter = list(Outbox.objects.filter(status="dead"))
for row in deadletter:
    print(row.event_type, row.last_error)
```

### Modos de ejecución

```python
# Loop bloqueante con SIGTERM/SIGINT handler
relay.run(handler=publish_to_kafka)

# Single-shot (testing / scheduler externo)
n_published = relay.drain_once(handler=publish_to_kafka)
```

## Receta: outbox + publisher Kafka

```python
import json
from kafka import KafkaProducer
from dorm.contrib.outbox import OutboxEvent, OutboxRelay

class Outbox(OutboxEvent):
    class Meta:
        db_table = "outbox"

producer = KafkaProducer(
    bootstrap_servers="kafka:9092",
    value_serializer=lambda v: json.dumps(v).encode(),
)

def publish(row):
    try:
        producer.send(
            row.event_type.split(".", 1)[0],   # topic = "order"
            row.payload,
        ).get(timeout=10)
        return True
    except Exception:
        return False

if __name__ == "__main__":
    relay = OutboxRelay(Outbox, batch_size=200)
    relay.run(handler=publish)
```

## Versus alternativas

| Patrón | Cuándo |
|---|---|
| **Outbox** (este) | Garantía de entrega + atomic con tx. Worker separado |
| **CDC** (logical replication) | Sin tabla extra; lee directamente del WAL. Más complejo |
| **LISTEN/NOTIFY** ([listen-notify](listen-notify.md)) | Real-time low-volume sin garantías de persistencia |

## Pitfalls

- **Tabla outbox crece sin parar**: añade un cron que purgue
  rows `published` viejas (>7 días).
- **Handler no idempotente**: si SKIP LOCKED no está disponible
  y dos relays se solapan, mismo evento se publica dos veces.
  Idempotency keys en el broker o post-checks evitan duplicados.
- **Workload spike**: el relay procesa secuencial. Si la cola
  crece más rápido que tu throughput, escala horizontalmente
  (varios procesos del relay) — SKIP LOCKED garantiza disjoint.

## Más

- [Helpers](helpers.md#outbox-pattern)
- [Idempotency keys](idempotency.md) — primitivo relacionado
- API: `dorm.contrib.outbox`
