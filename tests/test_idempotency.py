"""Tests for ``dorm.contrib.idempotency``."""

from __future__ import annotations

import pytest

from dorm.contrib.idempotency import (
    IdempotencyRecord,
    idempotency_key,
    purge_expired,
)
from dorm.db.connection import get_connection
from dorm.migrations.operations import _field_to_column_sql


class _IdpTbl(IdempotencyRecord):
    class Meta:
        db_table = "idp_tbl"
        app_label = "tests"


@pytest.fixture(autouse=True)
def _table():
    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "idp_tbl"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in _IdpTbl._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "idp_tbl" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "idp_tbl"{cascade}')


def test_first_call_replay_is_false():
    with idempotency_key("k1", model=_IdpTbl) as ctx:
        assert ctx.replay is False
        assert ctx.cached_response is None
        ctx.store({"id": 1, "ok": True}, status_code=201)


def test_second_call_replays():
    with idempotency_key("k2", model=_IdpTbl) as ctx:
        ctx.store({"value": 42}, status_code=200)

    with idempotency_key("k2", model=_IdpTbl) as ctx:
        assert ctx.replay is True
        assert ctx.cached_response == {"value": 42}
        assert ctx.cached_status_code == 200


def test_empty_key_rejected():
    with pytest.raises(ValueError, match="non-empty"):
        with idempotency_key("", model=_IdpTbl):
            pass


def test_store_idempotent():
    with idempotency_key("k3", model=_IdpTbl) as ctx:
        ctx.store({"a": 1})
        ctx.store({"a": 2})  # ignored — already stored
    with idempotency_key("k3", model=_IdpTbl) as ctx:
        assert ctx.cached_response == {"a": 1}


def test_non_serialisable_response_rejected():
    class _Bad:
        pass

    with pytest.raises(ValueError, match="not JSON-serialisable"):
        with idempotency_key("k-bad", model=_IdpTbl) as ctx:
            ctx.store({"x": _Bad()})


def test_skipped_store_does_not_persist():
    with idempotency_key("k-skip", model=_IdpTbl) as ctx:
        # Caller chose NOT to call ctx.store — next call must rerun.
        assert ctx.replay is False

    with idempotency_key("k-skip", model=_IdpTbl) as ctx:
        assert ctx.replay is False  # still no cached row


def test_atomic_rollback_does_not_persist_key():
    """A failure inside the with-block reverts the idempotency
    insert too — next retry must see a clean slate."""

    class _Boom(Exception):
        pass

    with pytest.raises(_Boom):
        with idempotency_key("k-rollback", model=_IdpTbl) as ctx:
            ctx.store({"a": 1})
            raise _Boom()

    with idempotency_key("k-rollback", model=_IdpTbl) as ctx:
        assert ctx.replay is False


def test_purge_expired():
    import time

    with idempotency_key("kp1", model=_IdpTbl) as ctx:
        ctx.store({"v": 1})
    with idempotency_key("kp2", model=_IdpTbl) as ctx:
        ctx.store({"v": 2})
    time.sleep(1.0)
    n = purge_expired(_IdpTbl, older_than_seconds=0)
    assert n == 2
    assert _IdpTbl.objects.count() == 0
