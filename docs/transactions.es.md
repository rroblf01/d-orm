# Transacciones

dorm expone `transaction.atomic` (sync) y `transaction.aatomic`
(async). Ambos se pueden usar como context manager o como decorador,
y ambos anidan usando SAVEPOINTs para que un fallo interno no haga
rollback del bloque externo.

## Uso

```python
from dorm import transaction

# Context manager
with transaction.atomic():
    author = Author.objects.create(name="Alice", age=30)
    Book.objects.create(title="...", author=author)

# Decorador
@transaction.atomic
def transfer(src_id: int, dst_id: int, amount: int) -> None:
    Account.objects.filter(pk=src_id).update(balance=F("balance") - amount)
    Account.objects.filter(pk=dst_id).update(balance=F("balance") + amount)

# Por alias
@transaction.atomic("replica_writer")
def write_to_replica() -> None:
    ...
```

La semántica replica la de Django: cualquier excepción que se lance
dentro del bloque dispara rollback; salir limpiamente hace commit.

## Async

```python
from dorm.transaction import aatomic

async with aatomic():
    a = await Author.objects.acreate(name="Alice", age=30)
    await Book.objects.acreate(title="...", author=a)

@aatomic
async def transfer(...): ...

@aatomic("replica_writer")
async def replica_op(...): ...
```

Los bloques aatomic toman una conexión async del pool, así que la
puedes mantener a través de `await` sin bloquear el event loop.

## Anidamiento y savepoints

```python
with transaction.atomic():           # BEGIN
    Author.objects.create(name="A")

    try:
        with transaction.atomic():   # SAVEPOINT
            Author.objects.create(name="B")
            raise RuntimeError("rollback inner")
    except RuntimeError:
        pass                         # ROLLBACK TO SAVEPOINT — A sigue viva

    Author.objects.create(name="C")  # commitea junto con A
# COMMIT
```

Los autores A y C acaban persistidos; B se revierte al savepoint.
Útil para sub-pasos "best-effort" dentro de una transacción mayor.

## Elegir bien el límite

Mantén las transacciones **cortas** y **centradas en escrituras**:

- Un request que hace N lecturas y 1 escritura solo necesita la
  escritura dentro de `atomic()`.
- Las transacciones largas mantienen row locks → otros writers se
  bloquean → la cola de latencia se dispara.
- No envuelvas handlers HTTP enteros en `atomic()` "por si acaso".
  Un timeout de red o una llamada a una API externa dentro del
  bloque mantiene la transacción abierta todo ese tiempo.

## Réplicas de lectura: `using=`

Si tu `DATABASES` tiene varios alias, `atomic("alias")` ejecuta la
transacción en un pool de conexiones concreto. Así mantienes las
escrituras en un primary y las lecturas en una réplica sin confundir
el estado transaccional.

```python
@transaction.atomic("primary")
def create_post(...):
    Post.objects.using("primary").create(...)
```

`Manager.using(alias)` y `QuerySet.using(alias)` enrutan una única
query; `transaction.atomic(alias)` enruta el bloque entero.

Para reglas de enrutado a nivel de app, mira el setting
`DATABASE_ROUTERS` en [Despliegue en producción](production.md).

## Auto-commit y transacciones explícitas

dorm corre en **auto-commit por defecto** — cada sentencia fuera de
un bloque `atomic()` commitea inmediatamente. No necesitas envolver
lecturas simples o escrituras de una sola sentencia; `atomic()`
existe para los casos en que múltiples sentencias tienen que
"triunfar o fracasar" como una unidad.

## A nivel de conexión vs a nivel de alias

Algunas cosas que conviene saber del modelo:

- `atomic()` saca una conexión del pool, abre una transacción,
  ejecuta tu código, y commitea/revierte al salir del bloque.
- Llamadas anidadas a `atomic()` sobre el mismo alias reutilizan la
  conexión y emiten `SAVEPOINT` / `RELEASE SAVEPOINT` /
  `ROLLBACK TO SAVEPOINT` en lugar de nuevos `BEGIN` / `COMMIT`.
- Llamadas concurrentes en alias distintos van a pools distintos —
  son transacciones independientes y **dorm no las coordina**. Si
  necesitas atomicidad cross-DB de verdad, hazlo en la capa de
  aplicación con sagas / outbox.

## Pitfalls

- **Mezclar sync y async en el mismo alias dentro de un request**:
  el `atomic()` sync y el `aatomic()` async pasan por pools
  distintos. Una sentencia en uno es invisible para la transacción
  que corre en el otro.
- **Capturar excepciones dentro del bloque y esperar commit**:
  tragarse una excepción aún commitea — `atomic()` solo hace
  rollback con excepciones que *salen* del bloque.
- **Mantener un `atomic()` largo alrededor de I/O externa**: deja
  los locks abiertos durante la parte lenta. Saca la I/O fuera del
  bloque cuando puedas.
