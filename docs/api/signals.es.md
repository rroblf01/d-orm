# Señales

Sistema mínimo de señales para hooks de save/delete/query.

> Las firmas son código Python; la prosa es traducción manual de las
> docstrings originales en inglés.

## `Signal`

```python
class Signal:
    def connect(
        self,
        receiver: Callable,
        sender: type | None = None,
        weak: bool = True,
        dispatch_uid: str | None = None,
    ) -> None: ...

    def disconnect(
        self,
        receiver: Callable | None = None,
        sender: type | None = None,
        dispatch_uid: str | None = None,
    ) -> bool: ...

    def send(self, sender: Any, **kwargs: Any) -> list[tuple[Callable, Any]]: ...
```

Despachador minimalista de eventos.

### Registración

| Argumento | Efecto |
|---|---|
| `receiver` | callable a invocar; firma `def fn(sender, **kwargs)` |
| `sender` | filtra por sender — solo se invoca si `sender is filt_sender` |
| `weak` | guarda una WeakRef al receiver (default `True`); evita fugas si el handler es un método de instancia que se garbage-collecta. Pasa `False` para tenerlo "vivo para siempre" |
| `dispatch_uid` | clave estable de identidad — `connect()` con un uid existente reemplaza al anterior. Útil para registración idempotente en módulos que se importan dos veces |

### Despacho

`send(sender=..., **kwargs)` invoca cada receiver y devuelve
`list[(handler, return_value)]`. **Las excepciones de un handler se
silencian** — un handler defectuoso no rompe la cascada. Los handlers
con WeakRef cuyo target ya fue garbage-collectado se eliminan en el
siguiente `send`.

## Señales pre-definidas

```python
from dorm.signals import (
    pre_save, post_save,
    pre_delete, post_delete,
    pre_query, post_query,
)
```

### `pre_save` / `post_save`

Disparan antes/después de `save()` (sync y async). Argumentos:
`sender=ModelClass`, `instance=ModelInstance`, `created: bool` (solo
post_save), `using=alias`.

```python
def slugify(sender, instance, **kwargs):
    if not instance.slug:
        instance.slug = slugify(instance.title)

pre_save.connect(slugify, sender=Article)
```

### `pre_delete` / `post_delete`

Disparan antes/después de `delete()`. Argumentos: `sender`,
`instance`, `using`. `pre_delete` es el momento natural para limpiar
recursos externos (S3, caches, búsquedas) antes de que la fila
desaparezca.

### `pre_query` / `post_query`

Observabilidad: una señal por SQL ejecutado, sync o async.

- **`pre_query`** — `sender=<vendor>` (`"sqlite"` / `"postgresql"`),
  `sql`, `params`.
- **`post_query`** — `sender=<vendor>`, `sql`, `params`,
  `elapsed_ms`, `error` (la excepción lanzada o `None`).

Conéctalas a tu APM / OpenTelemetry / structlog para trazar latencia
de queries y detectar slow queries:

```python
from dorm.signals import pre_query, post_query

def trace(sender, sql, params, **kwargs):
    log.info("query", sql=sql, ms=kwargs.get("elapsed_ms"))

post_query.connect(trace)
```

Los receivers deberían ser baratos — el trabajo pesado va en otra
parte (queue / worker).

---

> Para la versión auto-generada desde docstrings (en inglés), mira
> [Signals (English)](../../api/signals/).
