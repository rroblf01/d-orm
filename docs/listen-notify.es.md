# LISTEN / NOTIFY (PostgreSQL pub/sub)

`dorm.contrib.listen_notify` (3.4+) wrappa el primitivo de
publicación/suscripción de PostgreSQL en una API async idiomática.
Sin broker, sin Redis — el canal vive sobre la propia conexión
PG. Ideal para invalidación de caché, despertar workers, push
notifications low-volume.

PostgreSQL-only.

## Cuándo usarlo

- Invalidación de caché cross-proceso (no necesitas Redis).
- Despertar un job worker desde otro servicio sin colas externas.
- WebSocket fan-out con baja frecuencia (~docenas/seg).
- Coordinación entre réplicas que se conectan al mismo primary.

## Cuándo NO

- Fan-out alta frecuencia (>1k msg/seg) — los listeners ocupan
  conexiones PG dedicadas. Para volumen real usa NATS / Kafka /
  Redis Pub/Sub.
- Persistencia de mensajes — si el listener no está conectado,
  el NOTIFY se pierde. Para garantías de entrega usa el
  [outbox pattern](outbox.md).
- Payloads grandes — PG limita a 8000 bytes por NOTIFY.

## API

```python
from dorm.contrib.listen_notify import listen, notify, anotify

# Publisher (sync o async)
notify("orders", '{"id": 42}')
await anotify("orders", '{"id": 43}')

# Subscriber
async def consumer():
    async with listen("orders") as channel:
        async for n in channel:
            print(f"{n.channel} (pid={n.pid}): {n.payload}")
            if some_condition:
                break

# Múltiples canales
async with listen("orders", "cancellations") as channel:
    async for n in channel:
        if n.channel == "orders":
            handle_order(n.payload)
        elif n.channel == "cancellations":
            handle_cancel(n.payload)
```

`listen()` mantiene una conexión PG dedicada hasta que sales del
bloque. `notify()` corre como una query normal — puede ir dentro
de `atomic()` (los NOTIFY se entregan al COMMIT).

## Receta: invalidación de caché

```python
import json
from dorm.contrib.listen_notify import anotify, listen

# Servicio que escribe:
async def update_user(user_id: int, **fields):
    user = await User.objects.aget(pk=user_id)
    for k, v in fields.items():
        setattr(user, k, v)
    await user.asave()
    await anotify("user_changed", json.dumps({"pk": user_id}))

# Servicio que cachea:
async def cache_invalidator():
    async with listen("user_changed") as ch:
        async for n in ch:
            data = json.loads(n.payload)
            await redis.delete(f"user:{data['pk']}")
```

## Receta: WebSocket fan-out

```python
@app.websocket("/orders/feed")
async def feed(ws: WebSocket):
    await ws.accept()
    async with listen("orders") as ch:
        async for n in ch:
            await ws.send_text(n.payload)
```

Cada WebSocket abierto = una conexión PG dedicada. Para >100
WebSockets concurrentes, considera un broker dedicado.

## Caveats

- **Conexión dedicada per-listener**: el pool PG no se reutiliza.
  Cuenta esto en `MAX_POOL_SIZE`.
- **Payloads ≤ 8000 bytes**: PG hard-cap. Para mensajes largos
  envía solo el ID y el listener hace `aget(pk=…)`.
- **Auto-cleanup**: salir del `async with listen(...)` ejecuta
  `UNLISTEN` y devuelve la conexión al pool.

## Más

- [Helpers](helpers.md#listen-notify-async-helper)
- API: `dorm.contrib.listen_notify`
