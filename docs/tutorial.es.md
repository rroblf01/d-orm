# Tutorial: tu primera API con FastAPI en 5 minutos

Objetivo: una API `/users` minimalista con SQLite, async, tipos de
extremo a extremo. Cableamos dorm + FastAPI + Pydantic, generamos una
migración y golpeamos un endpoint real. No requiere conocimiento previo
de dorm.

## 1. Instalar

```bash
pip install "djanorm[sqlite,pydantic]" "fastapi[standard]"
# o con uv:
uv add "djanorm[sqlite,pydantic]" "fastapi[standard]"
```

## 2. Esqueleto del proyecto

```bash
dorm init --app users
```

Crea `settings.py`, `users/__init__.py` y `users/models.py` con un
modelo `User` de partida. Abre `settings.py` y descomenta el bloque
SQLite:

```python
DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": "db.sqlite3",
    }
}
```

## 3. Editar el modelo

`users/models.py`:

```python
import dorm

class User(dorm.Model):
    username = dorm.CharField(max_length=150, unique=True)
    email = dorm.EmailField(unique=True)
    age = dorm.IntegerField()
    is_active = dorm.BooleanField(default=True)
    created_at = dorm.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["username"]
```

## 4. Crear y aplicar la migración

```bash
dorm makemigrations
dorm migrate
```

Aparecen dos ficheros bajo `users/migrations/`. El schema ya es real.

## 5. Cablear la app FastAPI

Crea `main.py`:

```python
from fastapi import FastAPI, HTTPException
from pydantic import field_validator

import dorm
from dorm.contrib.pydantic import DormSchema

from users.models import User


app = FastAPI()


# ── Schemas ───────────────────────────────────────────────────────────────────

class UserOut(DormSchema):
    """Schema de respuesta — todas las columnas."""
    class Meta:
        model = User


class UserCreate(DormSchema):
    """POST body — sin auto-PK ni timestamps, pasa el email a minúsculas."""
    @field_validator("email")
    @classmethod
    def lower(cls, v: str) -> str:
        return v.lower()

    class Meta:
        model = User
        exclude = ("id", "created_at")


# ── Rutas ─────────────────────────────────────────────────────────────────────

@app.post("/users", response_model=UserOut, status_code=201)
async def create_user(payload: UserCreate) -> User:
    return await User.objects.acreate(**payload.model_dump())


@app.get("/users", response_model=list[UserOut])
async def list_users() -> list[User]:
    return await User.objects.all()


@app.get("/users/{user_id}", response_model=UserOut)
async def get_user(user_id: int) -> User:
    user = await User.objects.aget_or_none(pk=user_id)
    if user is None:
        raise HTTPException(404, "User not found")
    return user


@app.get("/healthz")
async def healthz() -> dict:
    return await dorm.ahealth_check()
```

## 6. Lanzarlo

```bash
fastapi dev
```

En otro terminal:

```bash
# Crear un usuario
curl -X POST localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"username":"alice","email":"ALICE@example.com","age":30}'

# Devuelve (fíjate en cómo el validator pasó el email a minúsculas):
# {"id":1,"username":"alice","email":"alice@example.com","age":30,
#  "is_active":true,"created_at":"2026-04-25T16:30:00"}

# Listar usuarios — un solo round-trip
curl localhost:8000/users

# Health check — listo para liveness/readiness probes de k8s
curl localhost:8000/healthz
# {"status":"ok","alias":"default","elapsed_ms":1.2}
```

## Lo que has obtenido gratis

- **Tipado**: `user.username` es `str`, no `Any`. Prueba `user.usernam`
  en tu editor — el IDE marca el typo.
- **Pool async listo para producción**: tamaños estilo psycopg-pool,
  reintento de errores transitorios, detección de queries lentas.
- **Validación en el borde**: `email: "no-vale"` lo rechaza
  `EmailField` antes de tocar la BD.
- **Schemas single-source-of-truth**: `DormSchema(Meta.model = User)`
  deriva el schema de FastAPI directamente del modelo dorm. Añade un
  campo al modelo y migra; la API lo recoge automáticamente.

## Siguientes pasos

- Cambia a PostgreSQL editando `DATABASES["default"]["ENGINE"]` — el
  resto del código no cambia. Ver [Despliegue en producción](production.md)
  para tunear el pool.
- Añade una relación uno-a-muchos: `posts = ForeignKey(User, ...)` en
  un modelo nuevo, `dorm makemigrations`, listo.
- Cablea métricas: conecta a `dorm.post_query` para timings por
  statement.
- Para schemas de respuesta anidados (p. ej. `User` con `Post[]`),
  ver la [guía de FastAPI](fastapi.md).
