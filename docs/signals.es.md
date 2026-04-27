# Señales

Las señales te permiten engancharte a eventos del ciclo de vida del
modelo (`save`, `delete`) y a cada query SQL sin acoplar esa lógica
al código del modelo. dorm trae seis señales integradas; la API
replica la de Django.

## Cuándo usar una señal (y cuándo no)

**Usa una señal cuando:**

- el hook es *transversal* — auditoría, invalidación de caché,
  indexado de búsqueda, tracing, métricas — y no quieres añadir una
  llamada de método en cada sitio que guarda;
- código de terceros necesita reaccionar a tus modelos sin
  modificarlos.

**No uses una señal cuando:**

- la lógica *pertenece* al modelo — sobrescribe `save()` /
  `clean()`. Las señales son acopladas-flojo a propósito y eso
  hace que el flujo de control sea más difícil de seguir;
- necesitas garantizar un valor de retorno o abortar la operación —
  las excepciones de `pre_save` se tragan (mira [Pitfalls](#pitfalls)).

## Las seis señales integradas

Todas viven en `dorm.signals`. Disparan idénticamente para
operaciones **sync y async**: los handlers son siempre callables
síncronos.

| Señal | Dispara | El `sender` es | Kwargs extra |
|---|---|---|---|
| `pre_save` | antes de que `save()` / `asave()` ejecute SQL | la **clase modelo** | `instance`, `raw=False`, `using`, `update_fields` |
| `post_save` | tras volver el INSERT/UPDATE | clase modelo | `instance`, `created` (bool), `raw=False`, `using`, `update_fields` |
| `pre_delete` | antes de que `delete()` / `adelete()` ejecute SQL | clase modelo | `instance`, `using` |
| `post_delete` | tras volver el DELETE | clase modelo | `instance`, `using` |
| `pre_query` | antes de cada sentencia SQL | el **string del vendor** (`"postgresql"` / `"sqlite"`) | `sql`, `params` |
| `post_query` | tras completar el SQL (o lanzar) | string del vendor | `sql`, `params`, `elapsed_ms`, `error` |

Algunas notas sobre los kwargs:

- **`instance`** es la instancia viva del modelo, no una copia —
  mutarla en `pre_save` *sí* se ve en el SQL siguiente. Es el
  patrón típico para "auto-fija un slug si falta".
- **`created`** en `post_save` es `True` si la fila se acaba de
  insertar, `False` para updates. Es la forma más limpia de
  distinguir los dos sin re-consultar.
- **`raw=False`** está reservado para futuro soporte de carga de
  fixtures; ahora siempre es `False`. Coincide con la firma de
  Django para que los handlers escritos para Django porten directos.
- **`using`** es el alias de BD que tocó la operación
  (`"default"`, `"replica"`...) — útil para handlers conscientes
  del routing.
- **`error`** en `post_query` es la excepción lanzada (o `None` si
  la sentencia tuvo éxito). Comprueba siempre `error` antes de
  tratar `elapsed_ms` como tiempo de query exitosa.

## Firma del receiver

Siempre dos partes: `sender` posicional, después `**kwargs`. Puedes
desempaquetar los kwargs que te interesen e ignorar el resto con
`**_`.

```python
def my_handler(sender, **kwargs):
    instance = kwargs["instance"]
    created = kwargs.get("created", False)
    ...
```

La razón del catch-all `**kwargs`: dorm puede añadir nuevos
keyword args a una señal en el futuro (mira `update_fields`, que se
añadió sin romper receivers anteriores). Un handler que liste cada
argumento explícitamente empezará a lanzar `TypeError` el día que
aparezca uno nuevo. **Acaba siempre la firma con `**kwargs`** (o
`**_` si ignoras todo lo que no sea `sender`).

## Conectar y desconectar

```python
from dorm.signals import post_save

def audit(sender, instance, created, **kw):
    AuditLog.objects.create(
        model=sender.__name__,
        pk=instance.pk,
        action="created" if created else "updated",
    )

post_save.connect(audit, sender=Article)
```

`Signal.connect(receiver, sender=None, weak=True, dispatch_uid=None)`:

| Argumento | Efecto |
|---|---|
| `receiver` | el callable; firma `def fn(sender, **kwargs)` |
| `sender` | solo invoca cuando `send()` se llamó con este sender. Patrón típico: `sender=Article` para que el handler solo dispare en saves de `Article`, no en cualquier modelo |
| `weak` | default `True`. dorm guarda una `WeakRef` al receiver, así un handler que es método de instancia desaparece automáticamente cuando su objeto dueño se garbage-collecta. Pasa `False` para funciones a nivel de módulo que quieras tener "vivas para siempre" (y para silenciar el warning de WeakMethod si tu handler es un método ligado cuyo dueño no puedes mantener vivo de otra forma) |
| `dispatch_uid` | una identidad string estable. Conectar *otra vez* con el mismo `dispatch_uid` **reemplaza** la registración previa. Úsalo para llamadas a `connect()` en top-level de módulo así una re-importación no registra dos veces |

Desconecta con cualquiera de:

```python
post_save.disconnect(audit)                 # por receiver
post_save.disconnect(sender=Article)         # todos los handlers de este sender
post_save.disconnect(dispatch_uid="audit-x")  # por uid
```

## Patrón decorador `@receiver`

dorm no trae un decorador `@receiver` (el de Django no añade nada
de comportamiento — solo llama a `signal.connect`). Puedes hacerlo
en dos líneas:

```python
def receiver(signal, **kwargs):
    def deco(fn):
        signal.connect(fn, **kwargs)
        return fn
    return deco

@receiver(post_save, sender=Article, dispatch_uid="reindex-articles")
def reindex(sender, instance, **kw):
    search.index(instance)
```

## Observabilidad con `pre_query` / `post_query`

Estas dos disparan en **cada** sentencia SQL — sync o async — así
que son el punto de integración para OpenTelemetry, Datadog,
structlog o cualquier cosa que necesite métricas por query.

```python
from dorm.signals import post_query

def trace(sender, sql, params, elapsed_ms, error, **kw):
    log.info(
        "query",
        vendor=sender,            # "postgresql" / "sqlite"
        ms=elapsed_ms,
        ok=error is None,
        sql=sql,
    )

post_query.connect(trace, weak=False, dispatch_uid="apm-trace")
```

Algunas reglas duras:

- **Mantén los handlers baratos.** Corren inline en el camino de la
  query. Un handler lento en `post_query` ralentiza cada llamada a
  BD. Si necesitas publicar métricas por la red, mete el trabajo
  en una cola (`asyncio.Queue`, ThreadPoolExecutor) y vuelve.
- **No emitas más queries desde dentro de una señal de query.** Es
  un bucle infinito. Si necesitas guardar muestras, añádelas a un
  ring buffer en memoria y persiste fuera de banda.
- **`error` es `None` en éxito.** Los handlers que siempre leen
  `elapsed_ms` para tiempo deberían comprobar
  `error is not None` antes de clasificar la llamada como "query
  lenta" — las queries fallidas suelen verse rápidas porque cortan
  el camino.

## Aviso sobre async

Las señales son **síncronas**. Disparan para operaciones sync y
async, pero el handler se invoca con una llamada de función plana
— **no** puedes `await` dentro.

```python
# Mal — la corrutina se crea y se descarta inmediatamente
async def bad(sender, **kw):
    await something_async()

post_save.connect(bad)   # nadie awaitea la corrutina; warnings por todas partes

# Bien — agéndala en el loop corriendo
def good(sender, instance, **kw):
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return       # estamos en contexto sync, cae a trabajo síncrono
    loop.create_task(do_something_async(instance))
```

Para la mayoría de casos de observabilidad, un encolado
no-bloqueante es suficiente; la I/O real ocurre en una task de
fondo que controlas.

## Efectos colaterales internos

dorm en sí **no** se suscribe a sus propias señales — existen
únicamente para código de usuario. Eso significa:

- Desactivar una señal (p.ej. `disconnect`-eando todos los
  receivers) nunca rompe operaciones del ORM.
- Un handler que lance no bloquea un save / delete / query — la
  excepción se loggea a `ERROR` en el logger `dorm.signals`, pero
  el código que llama continúa (mira más abajo).

## Manejo de fallos en receivers

Por defecto, una excepción lanzada por un receiver se loggea vía el
logger `dorm.signals` a nivel `ERROR` (con traceback completo) y
luego se suprime, así un único handler roto no puede tumbar un
camino de save/delete. Para conectarlo a tu stack de observabilidad:

```python
import logging

# Manda los fallos de señales de dorm a Sentry / DataDog / tu handler
logging.getLogger("dorm.signals").addHandler(tu_handler_alerta)
```

Si prefieres que la excepción propague — útil en tests, o para
señales personalizadas donde un handler fallido debería fallar la
operación — construye la señal con `raise_exceptions=True`:

```python
from dorm.signals import Signal

evento_estricto = Signal(raise_exceptions=True)
evento_estricto.connect(handler)
evento_estricto.send(sender=obj)   # cualquier error en un handler se relanza
```

Las señales internas (`pre_save`, `post_save`, `pre_delete`,
`post_delete`, `pre_query`, `post_query`) mantienen el
comportamiento legacy "loggear y suprimir" para preservar
compatibilidad.

## Pitfalls

- **Las excepciones de los handlers se loggean, no se tragan en
  silencio.** Un listener `post_save` con bug ya no desaparece en
  el vacío; queda registrado en el logger `dorm.signals` para que
  puedas enrutarlo a Sentry / tu alerta. Si quieres propagación
  estricta, usa una `Signal(raise_exceptions=True)` (ver arriba).
- **`pre_save` no puede abortar el save.** Lanzar dentro de
  `pre_save` se loggea pero el INSERT/UPDATE igual corre. Si
  necesitas vetar una operación, hazlo en `Model.clean()` (lo
  invoca `full_clean()`) o antes de llamar a `save()`.
- **Recursión.** Un handler `post_save` que llame a
  `instance.save()` re-dispara `pre_save` / `post_save` y puede
  buclear infinitamente. Usa `update_fields` para limitar el nuevo
  save (salta el re-fire para campos fuera de la lista si tienes
  cuidado), o protege con un flag thread-local.
- **La identidad del sender importa.** El filtro de `pre_save` usa
  comparación `is`: `connect(handler, sender=Article)` solo
  coincide con saves de `Article`, **no** con subclases de
  `Article`. Si tienes mixins abstractos (`TimestampedModel`),
  conecta a cada subclase concreta.
- **Re-importes de módulo doblan los handlers weak.** Si tu
  `connect()` vive en top-level de módulo y el módulo se recarga
  (Jupyter, hot-reload de dev), el handler queda registrado dos
  veces. Usa `dispatch_uid` para hacerlo idempotente.

## Referencia

API completa + kwargs por señal en la
[Referencia API](api/signals.md).
