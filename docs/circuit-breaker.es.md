# Circuit breaker

Cuando una BD lleva minutos caída, el retry agresivo agota pools y
sobrecarga el servidor cuando vuelve. El circuit breaker corta:
cuando detecta N fallos consecutivos, abre el circuito y todas las
llamadas rebotan inmediatamente con `CircuitOpenError`. Tras un
cooldown, prueba con una sola request (HALF_OPEN) y reabre si
falla.

`dorm.contrib.circuit_breaker` (3.4+).

## API

```python
from dorm.contrib.circuit_breaker import (
    CircuitBreaker, CircuitOpenError,
    circuit_breaker, reset_circuit_breakers, get_state,
)

# Singleton per-name (recomendado)
cb = circuit_breaker("default", failure_threshold=5, open_window_s=30.0)

def safe_count() -> int | None:
    try:
        with cb:
            return Author.objects.count()
    except CircuitOpenError:
        return None     # cache, default value, etc.

# Async
async def asafe_count() -> int | None:
    try:
        async with cb.aprotect():
            return await Author.objects.acount()
    except CircuitOpenError:
        return None
```

## Estados

```
                  failures ≥ threshold
   ┌───────┐  ────────────────────────▶ ┌────────┐
   │CLOSED │                            │  OPEN  │
   └───────┘  ◀──── reset / probe OK ── └────────┘
       ▲                                     │
       │                                     │ open_window_s elapsed
       │           ┌──────────────┐          │
       └───────────│  HALF_OPEN   │ ◀────────┘
       probe ok    └──────────────┘
                          │
                          │ probe fails
                          ▼
                     back to OPEN
```

- **CLOSED**: requests pasan. Cada fallo incrementa contador.
- **OPEN**: cada llamada raisea `CircuitOpenError` instantáneo.
  Tras `open_window_s`, transiciona a HALF_OPEN.
- **HALF_OPEN**: una llamada pasa para probar. Éxito → CLOSED.
  Fallo → OPEN con timer fresco.

## Configuración

```python
cb = circuit_breaker(
    "alias",
    failure_threshold=5,    # N fallos consecutivos para abrir
    open_window_s=30.0,     # tiempo en OPEN antes de HALF_OPEN
)
```

## Inspección

```python
state = get_state("alias")
# {"state": "open", "failures": 7, "opened_at": 12345.6}
```

Útil para Prometheus exporters / dashboards.

## Reset (tests)

```python
reset_circuit_breakers()    # drop every breaker
cb.reset()                  # reset just one
```

## Receta: con FastAPI

```python
from dorm.contrib.circuit_breaker import circuit_breaker, CircuitOpenError

@app.get("/users/{pk}")
async def get_user(pk: int):
    cb = circuit_breaker("default")
    try:
        async with cb.aprotect():
            user = await User.objects.aget(pk=pk)
    except CircuitOpenError:
        return JSONResponse(
            {"detail": "service unavailable"},
            status_code=503,
            headers={"Retry-After": "30"},
        )
    return user
```

## Pitfalls

- **Per-proceso**: cada worker mantiene su propio breaker. Un
  worker en OPEN no impide que otro intente. Para coordinación
  cross-worker, lleva el contador a Redis.
- **Tunes prematuros**: `failure_threshold=1` rebota a la primera
  falla flake. Mide tu tasa de errores real antes de ajustar.
- **Falsos positivos**: si tu app levanta `OperationalError` de
  forma legítima (e.g. `select_for_update` que tarda), el breaker
  cuenta esos. Filtra por excepción si hace falta.

## Más

- [Helpers](helpers.md#circuit-breaker)
- API: `dorm.contrib.circuit_breaker`
