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

## `atomic(durable=True)` (3.1+)

Pasa `durable=True` para afirmar que *este* bloque atomic es el
más externo — el código que lo rodea NO debe estar dentro de otro
`atomic()`. Lanza `RuntimeError` inmediato si degradaría
silenciosamente a savepoint:

```python
with transaction.atomic(durable=True):  # ok — top-level
    process_payment()
    schedule_emails()

# Error: durable anidado lanza en vez de ser un savepoint silencioso.
with transaction.atomic():
    with transaction.atomic(durable=True):  # RuntimeError
        ...
```

Úsalo cuando el trabajo DEBE aterrizar en su propio `COMMIT`
(patrones write-then-publish donde el publish espera un fsync
real, o donde un consumer downstream lee la fila por polling en
réplica). Espejo del flag Django añadido en 3.2.

La contraparte async `aatomic(durable=True)` aplica la misma
invariante a bloques `async with`.

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

## Efectos secundarios tras commit: `on_commit`

Mandar un email, encolar un job de Celery / RQ, publicar un mensaje
en Kafka, llamar a una API externa — efectos que **nunca** deben
disparar si su transacción padre se hace rollback. Envuélvelos en
`transaction.on_commit(callback)` para que solo corran tras un
commit exitoso:

```python
from dorm import transaction

with transaction.atomic():
    user = User.objects.create(name=name, email=email)
    transaction.on_commit(lambda: send_welcome_email(user))
    # Si algo falla más abajo, el user se hace rollback Y
    # el email no se manda. Quedan atómicamente acoplados.
    audit_log.record(user, action="signup")
```

Fuera de un bloque `atomic()`, `on_commit` ejecuta el callback
inmediatamente (paridad con Django). Bloques `atomic()` anidados
difieren todos los callbacks al commit del más externo — un rollback
a cualquier profundidad descarta los callbacks programados ahí.

Para código async usa `transaction.aon_commit`:

```python
from dorm import transaction

async with transaction.aatomic():
    user = await User.objects.acreate(name=name)
    transaction.aon_commit(lambda: notify_kafka(user))
    # las coroutines se await-ean en orden al commit más externo
```

`aon_commit` acepta tanto callables normales como coroutine
functions — estas últimas se await-ean en el momento del commit.

Un callback post-commit que falle se **loguea en el logger
`dorm.transaction` pero no se relanza**: para cuando corre, la BD ya
commiteó y propagar el error supondría reportar falsamente que la
transacción falló. Cablea ese logger a tu alerting si el callback es
crítico para la corrección.

## Limpieza al rollback: `on_rollback`

El espejo de `on_commit` — programa un callback que dispara **solo**
cuando la transacción que lo rodea hace rollback. Úsalo para deshacer
efectos secundarios no transaccionales cuando el trabajo de BD no
sobrevivió: borrar un fichero que acabas de escribir a storage local /
S3, eliminar una clave de un cache, mandar un webhook
"la notificación previa ha sido revertida".

```python
from dorm import transaction

with transaction.atomic():
    user = User.objects.create(name=name)
    s3_key = upload_avatar(user, image_bytes)
    # Si algo de aquí en adelante levanta, la fila se revierte Y
    # se borra el avatar — atómicos juntos.
    transaction.on_rollback(lambda: s3.delete(s3_key))
    audit_log.record(user, action="signup")
```

La semántica es la inversa de `on_commit`:

- **Fuera de un bloque `atomic()`**, `on_rollback` es un no-op — no
  hay transacción que revertir, así que no hay nada que deshacer.
  (Espejo del "ejecuta inmediatamente" de `on_commit`: misma
  respuesta lógica, ya que la "transacción" es definitiva.)
- **Dentro de `atomic()` anidados**, los callbacks disparan cuando
  *su* bloque hace rollback. Un rollback de savepoint dispara solo
  los callbacks internos; un rollback exterior dispara los internos
  fusionados más los externos en orden.
- **Si el bloque que los rodea commitea**, los callbacks de rollback
  encolados se descartan.
- **Un callback de rollback que falle se loguea**, no se relanza —
  misma razón que `on_commit`. El rollback ya ocurrió; perder una
  limpieza no debe escalar a crash.

Para código async usa `transaction.aon_rollback`:

```python
from dorm import transaction

async with transaction.aatomic():
    user = await User.objects.acreate(name=name)
    s3_key = await aupload_avatar(user, image_bytes)
    transaction.aon_rollback(lambda: s3_async.delete(s3_key))
```

`aon_rollback` acepta callables normales y coroutine functions —
las corrutinas se await-ean en el momento del rollback.

### Usuario integrado: `FileField`

`FileField.pre_save` registra automáticamente un `on_rollback`
cuando escribe un fichero dentro de un bloque `atomic()`, así que
este patrón funciona solo:

```python
with transaction.atomic():
    doc = Document(name="report")
    doc.attachment = dorm.ContentFile(b"PDF body", name="r.pdf")
    doc.save()                     # escribe en storage, encola cleanup
    raise BusinessRuleViolation()  # fila + bytes revertidos juntos
```

Sin ficheros huérfanos en disco, sin claves huérfanas en S3 / MinIO.
El registro automático aplica solo si estás dentro de un `atomic()`
activo — los saves no transaccionales no cambian. Ver
[Modelos: Archivos](models.md#archivos) para detalles del backend
de storage.

## Forzar rollback sin lanzar excepción: `set_rollback`

El context manager de atomic expone `set_rollback(True)` para forzar
un rollback saliendo del bloque `with` con normalidad — pensado
sobre todo para fixtures de tests y patrones de "trabajo
especulativo":

```python
with transaction.atomic() as tx:
    Author.objects.create(name="especulativo")
    if not is_useful(...):
        tx.set_rollback(True)
    # El bloque sale sin excepción; el rollback igual ocurre,
    # la fila especulativa desaparece, y los callbacks de
    # on_commit pendientes se descartan.
```

El fixture `dorm.test.transactional_db` está construido sobre esto.

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
- **`execute_script()` cierra la transacción envolvente en SQLite**:
  el `executescript()` de SQLite siempre emite un `COMMIT` antes y
  después del script, así que llamar a
  `connection.execute_script(...)` dentro de `atomic()` / `aatomic()`
  cierra la transacción externa — las sentencias previas del bloque
  quedan committed y ya no se pueden revertir. Es una limitación de
  SQLite, no un bug de dorm. Usa `connection.execute(...)` (una
  sentencia) cuando necesites control transaccional completo.
  PostgreSQL no se ve afectado.
