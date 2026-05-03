# `dorm.contrib.asyncguard`

Surfaces llamadas ORM sync ejecutadas dentro de un event loop
activo — el patrón bug que convierte una ruta async rápida en
lenta porque cada otra request del worker queda bloqueada mientras
la query sync se ejecuta.

El guard engancha `pre_query` y recorre el call stack: una llamada
ORM sync dentro de una corrutina dispara la acción configurada
(``"warn"`` / ``"raise"`` / ``"raise_first"``); las llamadas async
`a*` legítimas pasan en silencio.

## Cuándo activar

- **Tests / dev**: ``"warn"`` (o ``"raise"`` para fallos duros) —
  pilla el bug en design time.
- **Producción**: dejar desactivado. El stack walking en cada
  query tiene coste pequeño; mejor pagar en dev para no shippear
  el patrón.

```python
# conftest.py — activar para todo el suite de tests
from dorm.contrib.asyncguard import enable_async_guard

def pytest_configure(config):
    enable_async_guard(mode="warn")
```

## Modos

| Modo             | Comportamiento                                                            |
|------------------|---------------------------------------------------------------------------|
| `"warn"`         | Un único `WARNING` por call site (deduplicado por template).              |
| `"raise"`        | Raisea :class:`SyncCallInAsyncContext` en cada infractor.                 |
| `"raise_first"`  | Raisea una vez, luego degrada a `"warn"` para no inundar logs.            |

`SyncCallInAsyncContext` hereda de :class:`BaseException` así pasa
por encima del `except Exception` de `Signal.send` y aflora como un
500 en vez de un warning loggeado-y-tragado.

## API

::: dorm.contrib.asyncguard.enable_async_guard
::: dorm.contrib.asyncguard.disable_async_guard
::: dorm.contrib.asyncguard.SyncCallInAsyncContext
