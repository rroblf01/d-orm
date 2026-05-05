# Outbox pattern

Solves the **dual-write problem**: when an action needs to update
the DB *and* publish an event, doing both directly opens a
window where the DB commits but the broker publish fails.
Inconsistency.

The fix: write the event into an `outbox` table **inside the same
transaction** as the business write. A separate worker drains the
outbox and publishes to the broker. Both commit atomically together.

`dorm.contrib.outbox` (3.4+). Backend-agnostic (PG, MySQL, SQLite,
libsql, DuckDB).

## API

```python
from dorm.contrib.outbox import OutboxEvent, OutboxRelay, record_event
from dorm import transaction

# 1. Define your outbox table by subclassing the abstract base
class Outbox(OutboxEvent):
    class Meta:
        db_table = "outbox"

# 2. In the handler — record + business write in the same tx
with transaction.atomic():
    order = Order.objects.create(...)
    record_event(Outbox, "order.created", {"order_id": order.id})

# 3. Standalone worker — drains the outbox
def publish_to_kafka(row):
    kafka_client.send("orders", row.payload)
    return True       # success → mark published

relay = OutboxRelay(Outbox, batch_size=100)
relay.run(handler=publish_to_kafka)    # blocks; SIGTERM to stop
```

## `OutboxEvent` model

Built-in columns:

| Column | Type | Purpose |
|---|---|---|
| `id` | UUID | PK |
| `event_type` | CharField(128) | Event type (`order.created`) |
| `payload` | JSONField | Arbitrary data |
| `status` | CharField(16) | `pending` / `published` / `dead` |
| `attempts` | IntegerField | Retry counter |
| `last_error` | TextField | Last error message |
| `created_at` | DateTimeField | Creation timestamp |
| `published_at` | DateTimeField | Publication timestamp |

Override in your subclass for extra columns or different types.

## `OutboxRelay`

```python
relay = OutboxRelay(
    Outbox,
    batch_size=100,         # rows per batch
    poll_interval_s=1.0,    # seconds between empty batches
    max_attempts=5,         # before marking 'dead'
    using="default",
)
```

### Concurrency: `SKIP LOCKED`

On PostgreSQL, `OutboxRelay` uses
`SELECT … FOR UPDATE SKIP LOCKED` so **N parallel relays pick
disjoint rows**. No external coordination.

On backends without SKIP LOCKED (SQLite, MySQL < 8) it falls back
to plain SELECT — multiple relays may duplicate work. Make the
handler **idempotent** (the right move regardless).

### Dead letter

Rows that fail `max_attempts` times are marked `status='dead'`.
The relay skips them — they're for manual inspection.

```python
deadletter = list(Outbox.objects.filter(status="dead"))
for row in deadletter:
    print(row.event_type, row.last_error)
```

### Run modes

```python
# Blocking loop with SIGTERM/SIGINT handler
relay.run(handler=publish_to_kafka)

# Single-shot (testing / external scheduler)
n_published = relay.drain_once(handler=publish_to_kafka)
```

## Recipe: outbox + Kafka publisher

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

## Versus alternatives

| Pattern | When |
|---|---|
| **Outbox** (this) | Delivery guarantee + atomic with tx. Separate worker |
| **CDC** (logical replication) | No extra table; reads straight from the WAL. More complex |
| **LISTEN/NOTIFY** ([listen-notify](listen-notify.md)) | Real-time low-volume without persistence guarantees |

## Pitfalls

- **Outbox table grows forever**: add a cron purging old
  `published` rows (>7 days).
- **Non-idempotent handler**: when SKIP LOCKED isn't available and
  two relays overlap, the same event publishes twice. Use
  idempotency keys at the broker or post-checks to dedupe.
- **Workload spike**: the relay processes sequentially. If the
  queue grows faster than your throughput, scale horizontally
  (multiple relay processes) — SKIP LOCKED guarantees disjoint
  picks.

## More

- [Helpers](outbox.md)
- [Idempotency keys](idempotency.md) — related primitive
- API: `dorm.contrib.outbox`
