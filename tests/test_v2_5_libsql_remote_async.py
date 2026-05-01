"""Tests for the libsql ASYNC remote/embedded-replica path.

Real sqld / Turso endpoints aren't in scope for the test
suite — we mock ``turso.sync.connect`` instead. The fake
connection satisfies the small subset of the sqlite3-shaped
contract the wrapper actually uses (``execute``, ``commit``,
``close``, ``executescript``, ``rowcount``, ``lastrowid``,
``row_factory``, ``sync``).

Verifies:

- ``_get_sync_conn`` opens the connection once even under
  ``asyncio.gather`` fan-out (asyncio.Lock + double-checked
  ``self._sync_conn``).
- ``execute`` / ``execute_write`` / ``execute_insert`` /
  ``execute_script`` all dispatch to the dedicated worker
  executor and return correct shapes.
- ``execute_script`` falls back to per-statement execution
  when the underlying connection lacks ``executescript``.
- ``close`` shuts down the executor and clears state.
- ``force_close_sync`` releases the connection synchronously.
- ``auth_token`` and ``experimental_features`` flags are
  forwarded to ``turso.sync.connect`` (and the
  ``experimental_features`` retry path drops the kwarg on a
  TypeError).
- ``LibSQLDatabaseWrapper.sync_replica`` calls ``conn.sync()``
  when ``SYNC_URL`` is configured and skips when not.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# Fake connection / cursor shapes that emulate the slice of
# sqlite3 + turso.sync.Connection the async wrapper exercises.
# ─────────────────────────────────────────────────────────────────────────────


class _FakeCursor:
    def __init__(self, rows: list[tuple] | None = None, lastrowid: int = 0) -> None:
        self._rows = list(rows or [])
        self.lastrowid = lastrowid
        self.rowcount = 1

    def fetchall(self) -> list[tuple]:
        return list(self._rows)


class _FakeConn:
    def __init__(self) -> None:
        self.executed: list[tuple[str, Any]] = []
        self.committed = 0
        self.closed = False
        self.scripts: list[str] = []
        self._next_rows: list[tuple] = []
        self._next_lastrowid = 1
        self.row_factory: Any = None
        # Default sync count (an embedded replica connection has
        # this attribute; remote-only doesn't, but pyturso always
        # exposes it on a sync.Connection).
        self.sync_calls = 0

    def execute(self, sql: str, params: Any = ()) -> _FakeCursor:
        self.executed.append((sql, params))
        return _FakeCursor(self._next_rows, self._next_lastrowid)

    def commit(self) -> None:
        self.committed += 1

    def close(self) -> None:
        self.closed = True

    def executescript(self, sql: str) -> None:
        self.scripts.append(sql)

    def sync(self) -> None:
        self.sync_calls += 1


class _FakeConnNoExecutescript(_FakeConn):
    """Variant that simulates a libsql client without
    ``executescript``."""

    def __getattribute__(self, name: str) -> Any:
        if name == "executescript":
            raise AttributeError("not exposed")
        return object.__getattribute__(self, name)


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────


def _make_wrapper(conn_factory: Any) -> Any:
    from dorm.db.backends.libsql import LibSQLAsyncDatabaseWrapper

    w = LibSQLAsyncDatabaseWrapper(
        {
            "ENGINE": "libsql",
            "NAME": "/tmp/fake-replica.db",
            "SYNC_URL": "https://libsql.example",
            "AUTH_TOKEN": "tok-1234",
        }
    )

    # Patch ``_import_turso_sync`` to return a stub module whose
    # ``connect`` returns the fake connection. Mock at the import
    # boundary so the wrapper's actual code path runs.
    class _StubTursoSync:
        @staticmethod
        def connect(database: str, **kwargs: Any) -> Any:
            return conn_factory(database, **kwargs)

    import dorm.db.backends.libsql as libsql_mod

    setattr(w, "_stub_module", _StubTursoSync)
    setattr(libsql_mod, "_import_turso_sync", lambda: _StubTursoSync)
    return w


@pytest.fixture(autouse=True)
def _restore_imports():
    """Restore the real ``_import_turso_sync`` after each test so
    later tests use the genuine pyturso path."""
    import dorm.db.backends.libsql as libsql_mod

    real = libsql_mod._import_turso_sync
    yield
    setattr(libsql_mod, "_import_turso_sync", real)


# ─────────────────────────────────────────────────────────────────────────────
# Lifecycle: open / close / force_close_sync
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remote_async_get_conn_uses_executor() -> None:
    """``SYNC_URL`` set → wrapper opens the sync client on the
    dedicated executor and stamps ``self._sync_conn``."""
    last_kwargs: dict[str, Any] = {}

    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        last_kwargs.update(kwargs)
        last_kwargs["database"] = database
        return _FakeConn()

    w = _make_wrapper(_factory)
    try:
        conn = await w._get_conn()
        assert isinstance(conn, _FakeConn)
        # Async-conn slot stays empty in remote mode.
        assert w._async_conn is None
        # Sync conn slot is the one populated.
        assert w._sync_conn is conn
        # Auth token / experimental flag forwarded.
        assert last_kwargs["remote_url"] == "https://libsql.example"
        assert last_kwargs["auth_token"] == "tok-1234"
        assert last_kwargs["experimental_features"] == "vector"
        # Database path is the local replica.
        assert last_kwargs["database"] == "/tmp/fake-replica.db"
        # Executor allocated only when remote mode was activated.
        assert w._executor is not None
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_remote_async_get_conn_concurrent_returns_same() -> None:
    """asyncio.Lock + double-checked _sync_conn: gather() of three
    coroutines must yield the same connection — no leak."""
    open_calls = {"n": 0}

    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        open_calls["n"] += 1
        return _FakeConn()

    w = _make_wrapper(_factory)
    try:
        results = await asyncio.gather(
            w._get_conn(), w._get_conn(), w._get_conn()
        )
        assert results[0] is results[1] is results[2]
        assert open_calls["n"] == 1
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_remote_async_close_drains_executor() -> None:
    captured: dict[str, Any] = {}

    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        c = _FakeConn()
        captured["conn"] = c
        return c

    w = _make_wrapper(_factory)
    await w._get_conn()
    assert w._executor is not None
    await w.close()
    # Connection closed via the executor before shutdown.
    assert captured["conn"].closed is True
    assert w._sync_conn is None
    assert w._executor is None


@pytest.mark.asyncio
async def test_remote_async_force_close_sync() -> None:
    captured: dict[str, Any] = {}

    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        c = _FakeConn()
        captured["conn"] = c
        return c

    w = _make_wrapper(_factory)
    await w._get_conn()
    w.force_close_sync()
    assert captured["conn"].closed is True
    assert w._sync_conn is None
    assert w._executor is None


# ─────────────────────────────────────────────────────────────────────────────
# Query path
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remote_async_execute_returns_rows() -> None:
    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        c = _FakeConn()
        c._next_rows = [(1, "alpha"), (2, "beta")]
        return c

    w = _make_wrapper(_factory)
    try:
        rows = await w.execute("SELECT id, name FROM t WHERE id > %s", [0])
        assert rows == [(1, "alpha"), (2, "beta")]
        # ``%s`` adapted to ``?`` for the libsql wire layer.
        last_sql, _last_params = w._sync_conn.executed[-1]
        assert "?" in last_sql
        assert "%s" not in last_sql
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_remote_async_execute_write_commits_and_returns_rowcount() -> None:
    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        c = _FakeConn()
        c._next_rows = []
        return c

    w = _make_wrapper(_factory)
    try:
        n = await w.execute_write(
            "UPDATE t SET v = %s WHERE id = %s", [42, 1]
        )
        assert n == 1
        assert w._sync_conn.committed >= 1
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_remote_async_execute_insert_returns_lastrowid() -> None:
    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        c = _FakeConn()
        c._next_lastrowid = 99
        return c

    w = _make_wrapper(_factory)
    try:
        rid = await w.execute_insert(
            "INSERT INTO t (v) VALUES (%s)", [10]
        )
        assert rid == 99
        assert w._sync_conn.committed >= 1
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_remote_async_execute_script_uses_executescript() -> None:
    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        return _FakeConn()

    w = _make_wrapper(_factory)
    try:
        await w.execute_script(
            "CREATE TABLE u (id INTEGER PRIMARY KEY); CREATE TABLE v (id INTEGER PRIMARY KEY)"
        )
        assert w._sync_conn.scripts and "CREATE TABLE u" in w._sync_conn.scripts[0]
    finally:
        await w.close()


@pytest.mark.asyncio
async def test_remote_async_execute_script_falls_back_per_statement() -> None:
    """When the connection lacks ``executescript`` the wrapper
    splits on ``;`` and runs each statement via ``execute``."""

    def _factory(database: str, **kwargs: Any) -> _FakeConnNoExecutescript:
        return _FakeConnNoExecutescript()

    w = _make_wrapper(_factory)
    try:
        await w.execute_script(
            "CREATE TABLE u (id INTEGER PRIMARY KEY); CREATE TABLE v (id INTEGER PRIMARY KEY)"
        )
        # Each ``execute`` ran with no params (default ``()``).
        assert any(
            "CREATE TABLE u" in sql for sql, _ in w._sync_conn.executed
        )
        assert any(
            "CREATE TABLE v" in sql for sql, _ in w._sync_conn.executed
        )
    finally:
        await w.close()


# ─────────────────────────────────────────────────────────────────────────────
# Connect-kwarg fallback (experimental_features removed on TypeError)
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.asyncio
async def test_remote_async_drops_experimental_features_on_typeerror() -> None:
    """Forward-compat: a future pyturso may rename / drop the
    ``experimental_features`` kwarg. Verify the wrapper retries
    without it instead of crashing the user's app."""
    seen_kwargs: list[dict] = []

    def _factory(database: str, **kwargs: Any) -> _FakeConn:
        seen_kwargs.append(dict(kwargs))
        if "experimental_features" in kwargs:
            raise TypeError("future pyturso doesn't accept this")
        return _FakeConn()

    w = _make_wrapper(_factory)
    try:
        await w._get_conn()
        # Two attempts: first with experimental_features, second
        # without. The wrapper must have caught the TypeError.
        assert len(seen_kwargs) == 2
        assert "experimental_features" in seen_kwargs[0]
        assert "experimental_features" not in seen_kwargs[1]
    finally:
        await w.close()


# ─────────────────────────────────────────────────────────────────────────────
# sync_replica on the SYNC wrapper (mocked).
# ─────────────────────────────────────────────────────────────────────────────


def test_sync_replica_calls_sync_when_url_set() -> None:
    """The sync wrapper's ``sync_replica`` must call ``conn.sync()``
    when ``SYNC_URL`` is configured. Stub the connection factory
    so we don't need a real sqld endpoint."""
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    fake = _FakeConn()
    w = LibSQLDatabaseWrapper(
        {
            "ENGINE": "libsql",
            "NAME": ":memory:",
            "SYNC_URL": "https://libsql.example",
            "AUTH_TOKEN": "tok",
        }
    )
    w._local.conn = fake
    with w._conns_lock:
        from typing import cast as _cast
        _cast(dict, w._conns)[0] = fake
    w.sync_replica()
    assert fake.sync_calls == 1


def test_sync_replica_no_op_without_sync_url() -> None:
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    fake = _FakeConn()
    w = LibSQLDatabaseWrapper(
        {"ENGINE": "libsql", "NAME": ":memory:"}
    )
    w._local.conn = fake
    with w._conns_lock:
        from typing import cast as _cast
        _cast(dict, w._conns)[0] = fake
    w.sync_replica()
    # No call — local-only mode.
    assert fake.sync_calls == 0


def test_sync_replica_swallows_exception() -> None:
    """Network blip during sync must NOT propagate."""
    from dorm.db.backends.libsql import LibSQLDatabaseWrapper

    class _Boom(_FakeConn):
        def sync(self) -> None:
            raise RuntimeError("network blip")

    w = LibSQLDatabaseWrapper(
        {
            "ENGINE": "libsql",
            "NAME": ":memory:",
            "SYNC_URL": "https://libsql.example",
        }
    )
    fake = _Boom()
    w._local.conn = fake
    with w._conns_lock:
        from typing import cast as _cast
        _cast(dict, w._conns)[0] = fake
    w.sync_replica()  # must not raise
