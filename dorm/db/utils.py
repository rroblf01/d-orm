from __future__ import annotations

import contextvars
import logging
import os
import re
import time
from contextlib import contextmanager

_HINT = (
    "It looks like you forgot to create or apply your migrations.\n\n"
    "  Run the following commands:\n"
    "    dorm makemigrations\n"
    "    dorm migrate\n\n"
    "  Or, if you use a custom settings module:\n"
    "    dorm makemigrations --settings=<your_settings_module>\n"
    "    dorm migrate        --settings=<your_settings_module>\n"
)

# ContextVar shared by all async backends.
# Value: (wrapper_instance, connection, nesting_depth) or None.
# Each backend checks `state[0] is self` so multiple databases don't interfere.
ASYNC_ATOMIC_STATE: contextvars.ContextVar = contextvars.ContextVar(
    "dorm_async_atomic_state", default=None
)

# ── Query logging ─────────────────────────────────────────────────────────────
# Enable with `logging.getLogger("dorm.db").setLevel(logging.DEBUG)`.
# Slow-query threshold (ms) resolved from ``settings.SLOW_QUERY_MS`` first,
# then env var ``DORM_SLOW_QUERY_MS``, then the 500 ms default. ``None``
# disables the warning entirely (the comparison itself is skipped).

_slow_log = logging.getLogger("dorm.db")


# Sentinel for the unresolved-cache state. ``None`` is a valid resolved
# value (it disables the slow-query warning), so we can't use ``None``
# as the sentinel for "not yet computed". A unique object instance is
# unambiguous and survives ``configure(SLOW_QUERY_MS=None)``.
_SLOW_QUERY_UNSET: object = object()
_SLOW_QUERY_MS_CACHE: float | None | object = _SLOW_QUERY_UNSET


def _resolve_slow_query_ms() -> tuple[float | None, bool]:
    """Read the threshold from settings → env → default.

    Returns ``(value, cacheable)``. ``cacheable`` is True only when the
    value came from an explicit ``configure(SLOW_QUERY_MS=…)`` call —
    in that case ``configure`` invalidates the cache when the setting
    changes again. The env-var / default branch is NOT cached so a
    process that reads ``DORM_SLOW_QUERY_MS`` from a mutable
    environment (test monkeypatch, runtime tweak) keeps observing the
    current value without an explicit invalidation.
    """
    try:
        from ..conf import settings

        explicit = getattr(settings, "_explicit_settings", set())
        if "SLOW_QUERY_MS" in explicit:
            val = settings.SLOW_QUERY_MS
            if val is None:
                return None, True
            try:
                return float(val), True
            except (TypeError, ValueError):
                pass
    except Exception:
        pass

    raw = os.environ.get("DORM_SLOW_QUERY_MS")
    if raw is not None:
        try:
            return float(raw), False
        except (TypeError, ValueError):
            pass

    return 500.0, False


def _slow_query_ms() -> float | None:
    """Slow-query threshold (ms). Returns ``None`` to disable warnings.

    Hot-path read on every executed statement. Settings-derived values
    are memoised; env-var / default values fall through to a fresh
    resolution each call (same cost as before — a single
    ``os.environ.get``). ``configure(SLOW_QUERY_MS=…)`` invalidates the
    cache so a runtime swap takes effect on the next query.
    """
    global _SLOW_QUERY_MS_CACHE
    cached = _SLOW_QUERY_MS_CACHE
    if cached is not _SLOW_QUERY_UNSET:
        # Narrow the union back down: only ``_SLOW_QUERY_UNSET`` lands
        # in the ``object`` arm. Once we've ruled it out, the value is
        # the cached float / None we wrote earlier.
        assert cached is None or isinstance(cached, float)
        return cached
    val, cacheable = _resolve_slow_query_ms()
    if cacheable:
        _SLOW_QUERY_MS_CACHE = val
    return val


def _invalidate_slow_query_cache() -> None:
    """Drop the memoised threshold. Called by ``conf.configure`` whenever
    ``SLOW_QUERY_MS`` is part of the kwargs so the next query reads the
    fresh value."""
    global _SLOW_QUERY_MS_CACHE
    _SLOW_QUERY_MS_CACHE = _SLOW_QUERY_UNSET


# ── Transient-error retry ─────────────────────────────────────────────────────
# DBs occasionally drop connections (network blip, server restart, RDS
# failover). Retrying *outside* a transaction is safe and recovers
# transparently. Retrying *inside* a transaction is NOT safe — committed
# state would be re-applied. Backends pass ``in_transaction=True`` to skip
# retry when atomic_depth > 0.

_TRANSIENT_RETRY_ATTEMPTS = int(os.environ.get("DORM_RETRY_ATTEMPTS", "3"))
_TRANSIENT_RETRY_BACKOFF = float(os.environ.get("DORM_RETRY_BACKOFF", "0.1"))


def _is_transient(exc: BaseException) -> bool:
    """Detect connection-level errors worth retrying. Programming errors,
    integrity errors, etc. are NOT transient and must propagate."""
    import sqlite3

    if isinstance(exc, sqlite3.OperationalError):
        msg = str(exc).lower()
        # SQLite "database is locked" is transient under contention.
        return "locked" in msg or "busy" in msg
    try:
        import psycopg
        # psycopg.OperationalError covers connection_failure, admin_shutdown,
        # crash_shutdown, cannot_connect_now, idle_in_transaction_timeout.
        # InterfaceError fires when the connection is unusable.
        if isinstance(exc, (psycopg.OperationalError, psycopg.InterfaceError)):
            return True
    except ImportError:
        pass
    return False


def with_transient_retry(
    func,
    *,
    in_transaction: bool = False,
    attempts: int | None = None,
    backoff: float | None = None,
):
    """Run ``func()`` with simple exponential-backoff retry on transient
    DB errors. Skips retries while inside a transaction (would re-apply
    already-committed work)."""
    n = attempts if attempts is not None else _TRANSIENT_RETRY_ATTEMPTS
    bo = backoff if backoff is not None else _TRANSIENT_RETRY_BACKOFF
    if in_transaction or n <= 1:
        return func()

    log = logging.getLogger("dorm.db")
    last_exc: BaseException | None = None
    for attempt in range(1, n + 1):
        try:
            return func()
        except Exception as exc:
            if not _is_transient(exc) or attempt >= n:
                raise
            last_exc = exc
            sleep_for = bo * (2 ** (attempt - 1))
            log.warning(
                "Transient DB error (attempt %d/%d, retrying in %.2fs): %s",
                attempt,
                n,
                sleep_for,
                exc,
            )
            time.sleep(sleep_for)
    # Unreachable, but keeps type checkers happy.
    if last_exc is not None:
        raise last_exc


async def awith_transient_retry(
    coro_factory,
    *,
    in_transaction: bool = False,
    attempts: int | None = None,
    backoff: float | None = None,
):
    """Async counterpart of :func:`with_transient_retry`. ``coro_factory``
    is a 0-arg callable that returns a fresh coroutine on each retry —
    coroutines can only be awaited once."""
    import asyncio

    n = attempts if attempts is not None else _TRANSIENT_RETRY_ATTEMPTS
    bo = backoff if backoff is not None else _TRANSIENT_RETRY_BACKOFF
    if in_transaction or n <= 1:
        return await coro_factory()

    log = logging.getLogger("dorm.db")
    last_exc: BaseException | None = None
    for attempt in range(1, n + 1):
        try:
            return await coro_factory()
        except Exception as exc:
            if not _is_transient(exc) or attempt >= n:
                raise
            last_exc = exc
            sleep_for = bo * (2 ** (attempt - 1))
            log.warning(
                "Transient DB error (attempt %d/%d, retrying in %.2fs): %s",
                attempt,
                n,
                sleep_for,
                exc,
            )
            await asyncio.sleep(sleep_for)
    if last_exc is not None:
        raise last_exc


# Column-name fragments that suggest a value is sensitive. We mask the
# corresponding parameter in DEBUG logs so credentials don't leak into
# log aggregators / shared dashboards. Matched against the SQL text
# (case-insensitive substring on column names appearing immediately
# before each placeholder), so this only kicks in when the column itself
# carries the secret — bulk inserts where the secret column is one
# among many still get masked at that column's position.
_SENSITIVE_COLUMN_PATTERNS = (
    "password",
    "passwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "authorization",
    "auth_token",
    "access_key",
    "private_key",
)


_INSERT_RE = re.compile(
    r"""
    \bINSERT \s+ INTO \s+
    (?:"[^"]+"|[A-Za-z_][A-Za-z0-9_]*)        # table name
    \s* \( \s*
    (?P<cols>[^)]+)                              # column list inside ()
    \s* \) \s*
    VALUES \s*
    """,
    re.IGNORECASE | re.VERBOSE,
)


def _columns_from_insert(sql: str) -> list[str] | None:
    """If ``sql`` is an ``INSERT INTO t (a, b, c) VALUES …`` statement,
    return ``["a", "b", "c"]`` (lowercased, unquoted). Otherwise None.
    The column list cycles per VALUES tuple — bulk inserts with N tuples
    just repeat the same alignment N times in :func:`_placeholder_column_index`.
    """
    m = _INSERT_RE.search(sql)
    if not m:
        return None
    cols_raw = m.group("cols")
    cols: list[str] = []
    for piece in cols_raw.split(","):
        piece = piece.strip().strip('"').strip()
        if not piece:
            continue
        cols.append(piece.lower())
    return cols or None


def _placeholder_column_index(sql: str) -> list[str | None]:
    """For each ``%s`` / ``$N`` / ``?`` placeholder in ``sql``, return the
    column name that the value is bound to (or ``None`` if we can't
    figure one out). Used by :func:`_mask_params` to selectively redact
    values bound to sensitive columns.

    Two forms are handled:

    1. ``WHERE col = ?`` / ``SET col = ?`` / ``col IN (?, ?, …)`` — the
       column sits right before the placeholder. Handled by walking back
       from each placeholder position.
    2. ``INSERT INTO t (a, b, c) VALUES (?, ?, ?), (?, ?, ?)`` — the
       column list precedes a ``VALUES`` clause; we cycle through it for
       each tuple of placeholders.

    Best-effort: a real SQL parser would be more accurate, but logging-
    time redaction doesn't justify pulling one in. We may miss some
    assignments (false negative → leak), but we never mask the wrong
    value (false positive → break debugging).
    """
    cols: list[str | None] = []
    insert_cols = _columns_from_insert(sql)
    insert_idx = 0

    # Find where VALUES starts so we know which placeholders are in the
    # tuple form (cycle through insert_cols) vs elsewhere in the SQL.
    values_start: int | None = None
    if insert_cols:
        m = re.search(r"\bVALUES\b\s*\(", sql, re.IGNORECASE)
        if m:
            values_start = m.end()

    # Pre-compute spans of ``<col> IN ( ... )`` clauses so we can carry
    # the column down to every placeholder inside, not just the first
    # one. Without this, ``WHERE password IN (?, ?, ?)`` would mask only
    # the first ``?`` and leak the other two — exactly the kind of
    # half-redaction that's worse than no redaction.
    in_spans: list[tuple[int, int, str]] = []  # (start, end, column)
    for m in re.finditer(
        r'"?([A-Za-z_][A-Za-z0-9_]*)"?\s*\bIN\s*\(',
        sql,
        re.IGNORECASE,
    ):
        # Walk forward from the open paren to find the matching close
        # paren (single-level — no nested parens in IN lists in our SQL).
        depth = 1
        j = m.end()
        while j < len(sql) and depth > 0:
            ch = sql[j]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            j += 1
        in_spans.append((m.end(), j, m.group(1).lower()))

    def _col_for_in(pos: int) -> str | None:
        for start, end, col in in_spans:
            if start <= pos < end:
                return col
        return None

    for match in re.finditer(r"\$\d+|%s|\?", sql):
        pos = match.start()
        if insert_cols and values_start is not None and pos >= values_start:
            cols.append(insert_cols[insert_idx % len(insert_cols)])
            insert_idx += 1
            continue

        in_col = _col_for_in(pos)
        if in_col is not None:
            cols.append(in_col)
            continue

        prefix = sql[:pos]
        # Strip trailing whitespace + operator (=, <, >, !=, IN, etc).
        m = re.search(
            r'"?([A-Za-z_][A-Za-z0-9_]*)"?\s*(?:=|<>|!=|<=|>=|<|>|\bIN\b\s*\(?|\bLIKE\b|\bILIKE\b)\s*$',
            prefix,
            re.IGNORECASE,
        )
        cols.append(m.group(1).lower() if m else None)
    return cols


def _is_sensitive_column(col: str | None) -> bool:
    if col is None:
        return False
    col_l = col.lower()
    return any(pat in col_l for pat in _SENSITIVE_COLUMN_PATTERNS)


def _mask_params(sql: str, params):
    """Return a copy of ``params`` with values bound to sensitive columns
    replaced by ``"***"``. Returns ``params`` unchanged if nothing to mask
    or if the shape isn't a flat list/tuple.
    """
    if not params:
        return params
    if not isinstance(params, (list, tuple)):
        return params
    cols = _placeholder_column_index(sql)
    if not any(_is_sensitive_column(c) for c in cols):
        return params
    masked = list(params)
    for i, col in enumerate(cols):
        if i >= len(masked):
            break
        if _is_sensitive_column(col):
            masked[i] = "***"
    return masked


@contextmanager
def log_query(vendor: str, sql: str, params=None):
    """Time a SQL statement and dispatch query observability signals.

    Emits DEBUG for every query, WARNING above the slow-query threshold
    (``settings.SLOW_QUERY_MS`` → env var ``DORM_SLOW_QUERY_MS`` → 500 ms
    default; ``None`` disables the warning entirely), and fires
    ``dorm.signals.pre_query`` / ``post_query`` so user code can wire
    metrics or tracing. Values bound to columns whose name suggests a
    credential (``password``, ``token``, ``api_key`` …) are redacted in
    DEBUG / slow-query log lines — see :func:`_mask_params`. Signal
    receivers still get the raw params; if you ship them to external
    sinks, you're responsible for additional sanitisation there.
    """
    # Lazy import to avoid the circular dorm.signals → dorm.db at startup.
    from ..signals import pre_query, post_query

    pre_query.send(sender=vendor, sql=sql, params=params)
    start = time.perf_counter()
    error: BaseException | None = None
    try:
        yield
    except BaseException as exc:
        error = exc
        raise
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000.0
        log = logging.getLogger(f"dorm.db.backends.{vendor}")
        if log.isEnabledFor(logging.DEBUG):
            safe_params = _mask_params(sql, params)
            log.debug("(%.2fms) %s; params=%r", elapsed_ms, sql, safe_params)
        threshold = _slow_query_ms()
        if threshold is not None and elapsed_ms >= threshold:
            log.warning(
                "slow query (%.2fms ≥ %.0fms): %s", elapsed_ms, threshold, sql
            )
        post_query.send(
            sender=vendor,
            sql=sql,
            params=params,
            elapsed_ms=elapsed_ms,
            error=error,
        )


def raise_migration_hint(exc: Exception) -> None:
    """Re-raise a missing-table error with a friendly hint."""
    from dorm.exceptions import OperationalError

    msg = str(exc)
    match = re.search(r"no such table: (\S+)", msg, re.IGNORECASE) or re.search(
        r'relation "([^"]+)" does not exist', msg, re.IGNORECASE
    )
    if match:
        raise OperationalError(
            f'Table "{match.group(1)}" does not exist.\n\n{_HINT}'
        ) from exc


def normalize_db_exception(exc: Exception) -> None:
    """Convert backend exceptions to dorm exceptions, then check migration hint."""
    import sqlite3
    from dorm.exceptions import IntegrityError, OperationalError, ProgrammingError

    # ── SQLite ────────────────────────────────────────────────────────────────
    if isinstance(exc, sqlite3.IntegrityError):
        raise IntegrityError(str(exc)) from exc
    if isinstance(exc, sqlite3.OperationalError):
        raise_migration_hint(exc)
        raise OperationalError(str(exc)) from exc
    if isinstance(exc, sqlite3.ProgrammingError):
        raise ProgrammingError(str(exc)) from exc
    if isinstance(exc, sqlite3.DatabaseError):
        raise_migration_hint(exc)
        raise ProgrammingError(str(exc)) from exc

    # ── PostgreSQL ────────────────────────────────────────────────────────────
    try:
        import psycopg.errors as pg_errors
        import psycopg as psycopg_mod

        if isinstance(exc, pg_errors.IntegrityError):
            raise IntegrityError(str(exc)) from exc
        if isinstance(exc, (pg_errors.SyntaxError, pg_errors.ProgrammingError)):
            raise ProgrammingError(str(exc)) from exc
        if isinstance(exc, psycopg_mod.OperationalError):
            raise_migration_hint(exc)
            raise OperationalError(str(exc)) from exc
        if isinstance(exc, psycopg_mod.DatabaseError):
            raise_migration_hint(exc)
            raise ProgrammingError(str(exc)) from exc
    except ImportError:
        pass

    raise_migration_hint(exc)
