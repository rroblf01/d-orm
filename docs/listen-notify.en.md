# LISTEN / NOTIFY (PostgreSQL pub/sub)

`dorm.contrib.listen_notify` (3.4+) wraps PostgreSQL's pub/sub
primitive in an idiomatic async API. No broker, no Redis — the
channel lives on the PG connection itself. Useful for cache
invalidation, waking workers, low-volume push notifications.

PostgreSQL-only.

## When to use

- Cross-process cache invalidation (no Redis needed).
- Waking a worker from another service without an external queue.
- Low-rate WebSocket fan-out (~dozens/sec).
- Coordination between replicas connected to the same primary.

## When NOT to

- High-rate fan-out (>1k msg/sec) — listeners occupy dedicated PG
  connections. For real volume use NATS / Kafka / Redis Pub/Sub.
- Message persistence — if the listener is disconnected the
  NOTIFY is lost. For delivery guarantees use the
  [outbox pattern](outbox.md).
- Large payloads — PG caps NOTIFY at 8000 bytes.

## API

```python
from dorm.contrib.listen_notify import listen, notify, anotify

# Publisher (sync or async)
notify("orders", '{"id": 42}')
await anotify("orders", '{"id": 43}')

# Subscriber
async def consumer():
    async with listen("orders") as channel:
        async for n in channel:
            print(f"{n.channel} (pid={n.pid}): {n.payload}")
            if some_condition:
                break

# Multiple channels
async with listen("orders", "cancellations") as channel:
    async for n in channel:
        if n.channel == "orders":
            handle_order(n.payload)
        elif n.channel == "cancellations":
            handle_cancel(n.payload)
```

`listen()` holds a dedicated PG connection until you exit the
block. `notify()` runs as a normal query — works inside
`atomic()` (NOTIFY messages deliver on COMMIT).

## Recipe: cache invalidation

```python
import json
from dorm.contrib.listen_notify import anotify, listen

# Writer service:
async def update_user(user_id: int, **fields):
    user = await User.objects.aget(pk=user_id)
    for k, v in fields.items():
        setattr(user, k, v)
    await user.asave()
    await anotify("user_changed", json.dumps({"pk": user_id}))

# Cache service:
async def cache_invalidator():
    async with listen("user_changed") as ch:
        async for n in ch:
            data = json.loads(n.payload)
            await redis.delete(f"user:{data['pk']}")
```

## Recipe: WebSocket fan-out

```python
@app.websocket("/orders/feed")
async def feed(ws: WebSocket):
    await ws.accept()
    async with listen("orders") as ch:
        async for n in ch:
            await ws.send_text(n.payload)
```

Each open WebSocket = one dedicated PG connection. For >100
concurrent WebSockets, switch to a dedicated broker.

## Caveats

- **Dedicated connection per listener**: PG pool is not reused.
  Account for this in `MAX_POOL_SIZE`.
- **Payloads ≤ 8000 bytes**: PG hard-cap. For long messages send
  just the ID and have the listener do `aget(pk=…)`.
- **Auto-cleanup**: exiting `async with listen(...)` issues
  `UNLISTEN` and returns the connection to the pool.

## More

- [Helpers](listen-notify.md)
- API: `dorm.contrib.listen_notify`
