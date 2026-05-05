# Paquetes hermanos: `djanorm-mypy` y `pytest-djanorm`

Dos integraciones imprescindibles del ecosistema viven en
paquetes propios, **no en el wheel principal**:

| Paquete | Repo | Instalación |
|---------|------|-------------|
| **`djanorm-mypy`** | [`djanorm-mypy/`](https://github.com/rroblf01/d-orm/tree/main/djanorm-mypy) | `pip install djanorm-mypy` |
| **`pytest-djanorm`** | [`pytest-djanorm/`](https://github.com/rroblf01/d-orm/tree/main/pytest-djanorm) | `pip install pytest-djanorm` |

Esta página explica **por qué** se sacaron — la decisión es
intencional, no accidente de organización.

## Razones de la separación

### 1. Cero deps obligatorias en el wheel principal

`djanorm` instalado con `pip install djanorm` solo tira:

- la stdlib;
- el driver del backend que pidas (`[sqlite]` / `[postgresql]` / …).

Si los plugins vivieran en `dorm.contrib.mypy` y
`dorm.contrib.pytest`, tendrías dos opciones malas:

1. **Imports condicionales** dentro de `dorm`: el código está,
   pero falla en runtime si no instalaste `mypy`/`pytest`.
   El usuario corriente paga el coste cognitivo (`ImportError`
   misterioso) sin obtener nada.
2. **Deps duras** en el wheel: cada `pip install djanorm` mete
   `mypy` (~6 MB) y `pytest` (~3 MB) en el contenedor de
   producción. Imágenes Docker más grandes, vulnerabilidades
   transitivas extra, sin ningún beneficio en runtime.

Sacarlos a paquetes hermanos hace la regla obvia:

```bash
# Solo prod
pip install "djanorm[postgresql]"

# Dev: además sumar plugins
pip install djanorm-mypy pytest-djanorm
```

### 2. Versionado independiente

`mypy` y `pytest` rompen API entre minor releases con cierta
regularidad. `djanorm-mypy 0.2` puede pinear `mypy>=1.13,<2`
mientras `djanorm-mypy 0.3` migra a `mypy>=2`.

Si el plugin viviese en el wheel principal, **cada bump de mypy
forzaría un release de `djanorm`**, lo que es absurdo: un cambio
en una herramienta de dev no debería gatillar un release del
ORM en producción. La separación rompe ese acoplamiento de
calendarios.

### 3. Ciclos de release distintos

- `djanorm` (core ORM) — release lento y conservador. Mover una
  query mal puede romper miles de aplicaciones; cada release
  pasa por suite completa contra SQLite/PG/MySQL/libsql.
- `djanorm-mypy` — iterativo, bug-fixes rápidos cuando mypy
  emite falsos positivos. Iteración mensual cómoda.
- `pytest-djanorm` — fixtures evolucionan con uso real; el
  release puede ir más rápido que el core.

Tres trenes, tres velocidades, tres `pyproject.toml`.

### 4. Compatibilidad cruzada

`djanorm-mypy` puede declarar
`dependencies = ["mypy>=1.13", "djanorm>=3.4"]`. Si más
adelante hubiera una versión `djanorm 4.x` con cambios en el
descriptor `Field[T]`, sale `djanorm-mypy 1.0` con
`djanorm>=4`. Un usuario en `djanorm 3.x` se queda con
`djanorm-mypy 0.x`, sin disrupciones.

Imposible hacer esto cuando los dos viven en el mismo wheel.

### 5. Auto-discovery limpio

`pytest-djanorm` registra sus fixtures vía `pytest11`
entry-point en su `pyproject.toml`:

```toml
[project.entry-points.pytest11]
djanorm = "pytest_djanorm.plugin"
```

Pytest descubre el plugin **solo** si `pytest-djanorm` está
instalado. No hay magia ni `conftest.py` ad-hoc en el wheel
principal: la suite de tests del usuario solo conoce los
fixtures cuando explícitamente pidió el paquete.

Lo mismo para `mypy`:

```toml
[tool.mypy]
plugins = ["djanorm_mypy"]
```

Si `djanorm-mypy` no está instalado, mypy avisa con un error
claro: *"Plugin not installed"*. No falla silenciosamente, no
arranca con un set de comprobaciones reducido.

### 6. Tamaño y superficie de seguridad

| Métrica | Sin extracción | Con extracción |
|---------|---------------|---------------|
| Wheel `djanorm` | +~9 MB deps | unchanged |
| Imports en cold-start prod | +`mypy`, +`pytest` | unchanged |
| CVEs heredados | suma los de mypy + pytest | unchanged |
| `djanorm-mypy` opt-in | n/a | sí |

En contenedores Lambda, Cloud Run, edge functions — donde el
arranque en frío y el tamaño de imagen importan — sumar 9 MB
por una herramienta que **nunca** se ejecuta en producción es
inaceptable.

## Cuándo NO mantener un paquete separado

La separación tiene un coste real: dos repos a publicar, dos
changelogs, dos pipelines CI. Vale la pena cuando:

- la dep es **dev-only** (mypy, pytest) — sí.
- la dep es **opt-in con poca audiencia** (un broker exotic) — sí.
- la dep es **runtime universal** (psycopg) — no, va al wheel.

Por eso el wheel principal sigue empaquetando los backends BD
(`bulk_copy`, `listen_notify`, etc.): son código del runtime
del ORM. Mientras que tooling de dev (test/type-check) sale.

## Migración desde versiones previas

Si en proyectos antiguos importaste algo como
`from dorm.contrib.testing import …` o
`from dorm.contrib.mypy import …` — esos módulos **nunca
existieron** públicamente, así que no hay break: solo asegúrate
de instalar los hermanos:

```bash
pip install pytest-djanorm djanorm-mypy
```

Y sigue las recetas en sus respectivos READMEs.

## Resumen

> Mantén el wheel pequeño. Saca a paquetes propios todo lo que
> sea opt-in dev tooling. Versiona cada uno por su cuenta.
> El usuario paga solo lo que usa.
