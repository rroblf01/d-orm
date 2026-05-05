# Idempotency keys

Cliente envía un pago. Red flap. Cliente reintenta. Sin
idempotency keys, el pago se ejecuta dos veces.

`dorm.contrib.idempotency` (4.0+) implementa el patrón
canonical-Stripe: el cliente envía un header
`Idempotency-Key: <UUID>` por operación lógica; la primera vez,
el servidor ejecuta y guarda la respuesta indexada por la key;
los reintentos devuelven la respuesta cacheada en lugar de
re-ejecutar.

## API

```python
from dorm.contrib.idempotency import (
    IdempotencyRecord, idempotency_key, purge_expired,
)

# 1. Define tu tabla
class IdpEntry(IdempotencyRecord):
    class Meta:
        db_table = "idempotency_entries"

# 2. Wrappa la lógica no-idempotente
with idempotency_key(request.headers["Idempotency-Key"], model=IdpEntry) as ctx:
    if ctx.replay:
        return ctx.cached_response, ctx.cached_status_code
    result = process_payment(...)
    ctx.store(result, status_code=201)
    return result, 201

# 3. Cron: purga rows viejas
purge_expired(IdpEntry, older_than_seconds=86400 * 7)   # 7 días
```

## Atomicidad

`idempotency_key()` envuelve el bloque en `atomic()` — la fila
de idempotency commitea **junto con** la escritura de negocio. Si
algo falla, ambos rollback.

```python
with idempotency_key(key, model=IdpEntry) as ctx:
    Order.objects.create(...)        # business write
    ctx.store({"order_id": order.pk})
    raise SomethingBad()
# → ambos revertidos. Cliente reintenta y vuelve a procesar.
```

## Race conditions

Dos requests simultáneos con la misma key:

1. Ambos hacen `SELECT` (miss).
2. Ambos ejecutan la lógica.
3. El primero hace `INSERT` → OK.
4. El segundo hace `INSERT` → `IntegrityError` por UNIQUE.

El `atomic()` rollback lo limpia. El cliente reintentará y verá
la respuesta cacheada del primero.

Para alta concurrencia, considera `select_for_update()` previo al
work — bloquea a todo retry concurrente hasta que el primero
acabe. Coste: serialización por key.

## Validación de payload

`ctx.store(response)` valida que `response` sea JSON-serializable
antes de persistir:

```python
with idempotency_key(key, model=IdpEntry) as ctx:
    ctx.store({"x": some_object})    # ValueError si no es JSON
```

Mejor fallar al guardar que al leer del cache horas después.

## Receta: FastAPI middleware

```python
from fastapi import Header, Request
from fastapi.responses import JSONResponse
from dorm.contrib.idempotency import idempotency_key

@app.post("/payments")
async def create_payment(
    body: PaymentIn,
    idempotency_key_header: str = Header(alias="Idempotency-Key"),
):
    with idempotency_key(idempotency_key_header, model=IdpEntry) as ctx:
        if ctx.replay:
            return JSONResponse(
                ctx.cached_response,
                status_code=ctx.cached_status_code or 200,
            )
        result = await process_payment(body)
        ctx.store(result, status_code=201)
        return JSONResponse(result, status_code=201)
```

## TTL purge

Las rows nunca se borran solas. Cron-job:

```bash
# Purga rows >7 días
0 3 * * * cd /app && python -c "
from dorm.contrib.idempotency import purge_expired
from myapp.models import IdpEntry
purge_expired(IdpEntry, older_than_seconds=604800)
"
```

7 días es agresivo si los clientes reintentan tras horas; 30 días
si retentas tras días. Trade-off: tabla crece vs. ventana
protección.

## Pitfalls

- **Olvidar `ctx.store()` en éxito**: el bloque commitea sin
  guardar la respuesta — siguiente reintento re-ejecuta.
  Mantente disciplinado.
- **Keys débiles**: si el cliente reusa la misma key para
  operaciones distintas, devuelves la respuesta vieja. UUID por
  operación, no por sesión.
- **Validación de payload pesada**: el `json.dumps` de un dict
  enorme tarda. Para responses gigantes, almacena solo un
  fingerprint y reconstruye on demand.

## Más

- [Helpers](idempotency.md)
- [Outbox pattern](outbox.md) — comparable, distinto problema
- API: `dorm.contrib.idempotency`
