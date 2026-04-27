# Transacciones

API de bloques transaccionales sync y async. Las firmas son código
Python; la prosa es traducción manual.

## `atomic`

```python
def atomic(using: str | Callable = "default") -> ContextManager | Callable
```

Envuelve un bloque de código en una transacción de BD. Funciona tanto
como context manager como decorador:

```python
import dorm

# Context manager
with dorm.transaction.atomic():
    ...

# Decorador (sin paréntesis — `using` recibe la función)
@dorm.transaction.atomic
def update_balance(...):
    ...

# Decorador con alias
@dorm.transaction.atomic("replica_writer")
def report(...):
    ...
```

**Semántica:**

- En éxito, hace `COMMIT` al salir del bloque.
- En excepción que sale del bloque, hace `ROLLBACK`.
- Las llamadas anidadas crean SAVEPOINTs, así que un fallo interno
  solo revierte el bloque interno (no el externo).

## `aatomic`

```python
def aatomic(using: str | Callable = "default") -> AsyncContextManager | Callable
```

Equivalente async de `atomic`. Mismo uso: como `async with` o como
decorador sobre funciones async.

```python
from dorm.transaction import aatomic

# Context manager
async with aatomic():
    a = await Author.objects.acreate(name="Alice", age=30)
    await Book.objects.acreate(title="...", author=a)

# Decorador
@aatomic
async def transfer(...):
    ...

# Por alias
@aatomic("replica_writer")
async def replica_op(...):
    ...
```

Los bloques `aatomic` toman una conexión async del pool, así que la
puedes mantener a través de varios `await` sin bloquear el event loop.

## `on_commit`

```python
def on_commit(callback: Callable[[], Any], using: str = "default") -> None
```

Programa un callable sin argumentos para ejecutarse **después** del
commit de la transacción que lo envuelve. Patrón canónico para
disparar efectos secundarios que no deben ocurrir si la transacción
hace rollback (mandar email, encolar Celery/RQ, publicar a Kafka,
llamar API externa).

```python
from dorm import transaction

with transaction.atomic():
    user = User.objects.create(name=name, email=email)
    transaction.on_commit(lambda: send_welcome_email(user))
    audit_log.record(user, action="signup")
# El email se manda solo si el commit triunfa.
```

**Semántica:**

- Fuera de un bloque `atomic()`, el callback corre inmediatamente
  (paridad con Django).
- Bloques `atomic()` anidados difieren todos los callbacks al commit
  más externo.
- Un rollback a cualquier profundidad descarta los callbacks
  programados ahí (y los merged de commits internos).
- Una excepción dentro del callback se loguea en el logger
  `dorm.transaction` pero **no** se relanza — la BD ya commiteó.

## `aon_commit`

```python
def aon_commit(
    callback: Callable[[], Any] | Callable[[], Awaitable[Any]],
    using: str = "default",
) -> None
```

Contraparte async. Acepta tanto callables regulares como coroutine
functions; las coroutines se await-ean al commit más externo.

```python
async with transaction.aatomic():
    user = await User.objects.acreate(name=name)
    transaction.aon_commit(lambda: notify_kafka(user))
```

Fuera de un bloque `aatomic()`, las coroutines se programan con
`asyncio.ensure_future` para que el sitio de llamada no tenga que
hacer await.

## Forzar rollback: `set_rollback`

El context manager devuelto por `atomic()` y `aatomic()` expone
`set_rollback(True)` para forzar un rollback al salir *sin lanzar
excepción*. Útil en fixtures de tests y patrones de "trabajo
especulativo":

```python
with dorm.transaction.atomic() as tx:
    Author.objects.create(name="especulativo", age=1)
    if not is_useful(...):
        tx.set_rollback(True)
    # Bloque sale limpio; el rollback igual ocurre.
```

Cuando se llama a `set_rollback(True)`, cualquier callback de
`on_commit` programado dentro del bloque se descarta — mismo
comportamiento que un rollback por excepción.

La variante async expone el mismo método en el context manager de
`aatomic()`, con semántica idéntica.

## Avisos

- **No mezcles sync y async sobre el mismo alias dentro de un mismo
  request.** El `atomic()` sync y el `aatomic()` async pasan por
  pools distintos; una sentencia en uno es invisible para la
  transacción del otro.
- **Capturar excepciones dentro del bloque y esperar commit:**
  tragarse una excepción aún commitea — `atomic()` solo hace
  rollback con excepciones que *salen* del bloque.
- **Mantener un `atomic()` largo alrededor de I/O externa**: deja
  los locks abiertos durante la parte lenta. Saca la I/O fuera del
  bloque cuando puedas.
- **`execute_script()` cierra la transacción envolvente en SQLite**:
  el `executescript()` de SQLite siempre emite `COMMIT` antes y
  después del script. Es una limitación de SQLite, no un bug. PG no
  se ve afectado. Mira la guía de [Transacciones](../transactions.md)
  para el detalle.

---

> Para la versión auto-generada desde docstrings (en inglés), mira
> [Transactions (English)](../../api/transaction/).
