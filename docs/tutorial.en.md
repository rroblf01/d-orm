# djanorm tutorial — your first FastAPI app in 5 minutes

Goal: a tiny `/users` API backed by SQLite, async, type-safe end to
end. We'll wire dorm + FastAPI + Pydantic, generate a migration, and
hit a real endpoint. No prior dorm knowledge needed.

## 1. Install

```bash
pip install "djanorm[sqlite,pydantic]" "fastapi[standard]"
# or, with uv:
uv add "djanorm[sqlite,pydantic]" "fastapi[standard]"
```

## 2. Scaffold the project

```bash
dorm init --app users
```

That creates `settings.py`, `users/__init__.py`, and `users/models.py`
with a starter `User` model. Open `settings.py` and uncomment the
SQLite block:

```python
DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": "db.sqlite3",
    }
}
```

## 3. Edit the model

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

## 4. Create and apply the migration

```bash
dorm makemigrations
dorm migrate
```

Two SQL files appear under `users/migrations/`. The schema is now real.

## 5. Wire the FastAPI app

Create `main.py`:

```python
from fastapi import FastAPI, HTTPException
from pydantic import field_validator

import dorm
from dorm.contrib.pydantic import DormSchema

from users.models import User



app = FastAPI()


# ── Schemas ───────────────────────────────────────────────────────────────────

class UserOut(DormSchema):
    """Response shape — every column."""
    class Meta:
        model = User


class UserCreate(DormSchema):
    """Request body — drop the auto-PK and timestamps, lower-case email."""
    @field_validator("email")
    @classmethod
    def lower(cls, v: str) -> str:
        return v.lower()

    class Meta:
        model = User
        exclude = ("id", "created_at")


# ── Routes ────────────────────────────────────────────────────────────────────

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

## 6. Run it

```bash
fastapi dev
```

In another terminal:

```bash
# Create a user
curl -X POST localhost:8000/users \
    -H 'content-type: application/json' \
    -d '{"username":"alice","email":"ALICE@example.com","age":30}'

# Returned (notice email got lower-cased by the validator):
# {"id":1,"username":"alice","email":"alice@example.com","age":30,
#  "is_active":true,"created_at":"2026-04-25T16:30:00"}

# List users — single round-trip
curl localhost:8000/users

# Health check — for k8s readiness probes
curl localhost:8000/healthz
# {"status":"ok","alias":"default","elapsed_ms":1.2}
```

## What you got for free

- **Type-safe**: `user.username` is `str`, not `Any`. Try `user.usernam`
  in your editor — your IDE flags the typo.
- **Async pool, ready for production**: psycopg-pool style sizing,
  retry on transient errors, slow-query detection.
- **Schema validation at the boundary**: `email: "not-an-email"` is
  rejected by `EmailField` before it reaches the DB.
- **Single-source-of-truth schemas**: `DormSchema(Meta.model = User)`
  derives the FastAPI schema directly from the dorm model. Add a field
  to the model and migrate; the API picks it up automatically.

