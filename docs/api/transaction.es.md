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
