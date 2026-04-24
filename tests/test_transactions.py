"""Tests for transaction support, thread-safe connections, and OPTIONS DSN."""
from __future__ import annotations

import threading


import dorm
from tests.models import Author


# ── Sync atomic ───────────────────────────────────────────────────────────────


def test_atomic_commit(clean_db):
    with dorm.transaction.atomic():
        Author.objects.create(name="Alice", age=30)
        Author.objects.create(name="Bob", age=25)
    assert Author.objects.count() == 2


def test_atomic_rollback(clean_db):
    try:
        with dorm.transaction.atomic():
            Author.objects.create(name="Alice", age=30)
            raise ValueError("intentional")
    except ValueError:
        pass
    assert Author.objects.count() == 0


def test_atomic_nested_savepoint(clean_db):
    """Inner failure rolls back only the inner block; outer block commits."""
    with dorm.transaction.atomic():
        Author.objects.create(name="Alice", age=30)
        try:
            with dorm.transaction.atomic():
                Author.objects.create(name="Bob", age=25)
                raise ValueError("inner failure")
        except ValueError:
            pass
        # Alice visible, Bob rolled back to savepoint
        assert Author.objects.count() == 1
        assert Author.objects.filter(name="Alice").exists()

    assert Author.objects.count() == 1


def test_atomic_full_rollback_on_outer_exception(clean_db):
    """If outer atomic raises, everything is rolled back."""
    try:
        with dorm.transaction.atomic():
            Author.objects.create(name="Alice", age=30)
            with dorm.transaction.atomic():
                Author.objects.create(name="Bob", age=25)
            raise RuntimeError("outer failure")
    except RuntimeError:
        pass
    assert Author.objects.count() == 0


# ── Async atomic ──────────────────────────────────────────────────────────────


async def test_aatomic_commit(clean_db):
    async with dorm.transaction.aatomic():
        await Author.objects.acreate(name="Alice", age=30)
        await Author.objects.acreate(name="Bob", age=25)
    assert await Author.objects.acount() == 2


async def test_aatomic_rollback(clean_db):
    try:
        async with dorm.transaction.aatomic():
            await Author.objects.acreate(name="Alice", age=30)
            raise ValueError("intentional")
    except ValueError:
        pass
    assert await Author.objects.acount() == 0


async def test_aatomic_nested_savepoint(clean_db):
    """Inner failure rolls back only the inner block; outer block commits."""
    async with dorm.transaction.aatomic():
        await Author.objects.acreate(name="Alice", age=30)
        try:
            async with dorm.transaction.aatomic():
                await Author.objects.acreate(name="Bob", age=25)
                raise ValueError("inner failure")
        except ValueError:
            pass
        assert await Author.objects.acount() == 1

    assert await Author.objects.acount() == 1


async def test_aatomic_full_rollback_on_outer_exception(clean_db):
    try:
        async with dorm.transaction.aatomic():
            await Author.objects.acreate(name="Alice", age=30)
            async with dorm.transaction.aatomic():
                await Author.objects.acreate(name="Bob", age=25)
            raise RuntimeError("outer failure")
    except RuntimeError:
        pass
    assert await Author.objects.acount() == 0


# ── get_or_create / update_or_create ─────────────────────────────────────────


def test_get_or_create_returns_existing(clean_db):
    Author.objects.create(name="Alice", email="alice@example.com", age=30)
    obj, created = Author.objects.get_or_create(
        email="alice@example.com",
        defaults={"name": "Duplicate", "age": 99},
    )
    assert not created
    assert obj.name == "Alice"
    assert Author.objects.count() == 1


def test_get_or_create_creates_new(clean_db):
    obj, created = Author.objects.get_or_create(
        email="new@example.com",
        defaults={"name": "New", "age": 20},
    )
    assert created
    assert obj.name == "New"


def test_update_or_create_updates_existing(clean_db):
    Author.objects.create(name="Alice", email="alice@example.com", age=30)
    obj, created = Author.objects.update_or_create(
        email="alice@example.com",
        defaults={"age": 31},
    )
    assert not created
    assert obj.age == 31
    assert Author.objects.count() == 1


async def test_aget_or_create_returns_existing(clean_db):
    await Author.objects.acreate(name="Alice", email="alice@example.com", age=30)
    obj, created = await Author.objects.aget_or_create(
        email="alice@example.com",
        defaults={"name": "Duplicate", "age": 99},
    )
    assert not created
    assert obj.name == "Alice"


# ── Thread-safe get_connection ────────────────────────────────────────────────


def test_get_connection_thread_safe(configure_dorm):
    """Multiple threads calling get_connection() must get the same wrapper."""
    from dorm.db.connection import get_connection

    results: list = []
    errors: list = []

    def worker():
        try:
            results.append(get_connection())
        except Exception as e:
            errors.append(e)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(results) == 20
    assert all(r is results[0] for r in results)


# ── OPTIONS DSN passthrough ───────────────────────────────────────────────────


def test_options_are_merged_into_dsn():
    from dorm.db.backends.postgresql import _build_dsn

    settings = {
        "HOST": "localhost",
        "PORT": 5432,
        "NAME": "mydb",
        "USER": "user",
        "PASSWORD": "pass",
        "OPTIONS": {"sslmode": "require", "connect_timeout": 10},
    }
    dsn = _build_dsn(settings)
    assert dsn["sslmode"] == "require"
    assert dsn["connect_timeout"] == 10


def test_options_empty_by_default():
    from dorm.db.backends.postgresql import _build_dsn

    dsn = _build_dsn({"HOST": "localhost", "NAME": "db"})
    assert "sslmode" not in dsn
