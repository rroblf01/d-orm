"""PostgreSQL two-phase commit (``PREPARE TRANSACTION``) helpers.

XA-style 2PC across multiple PG databases. The pattern:

1. Start one ``atomic()`` block per database alias.
2. ``PREPARE TRANSACTION '<gid>'`` on each connection (votes "yes").
3. If every prepare returned OK, ``COMMIT PREPARED '<gid>'`` on each
   (commit phase). Otherwise ``ROLLBACK PREPARED '<gid>'`` everywhere.

Use sparingly. 2PC has well-known availability trade-offs — if the
coordinator crashes between phase 1 and phase 2, prepared transactions
hold locks indefinitely. Prefer the saga pattern (see
:mod:`dorm.contrib.saga`) for everything that isn't strictly required
to be 2PC.

Example::

    from dorm.contrib.two_phase import two_phase_commit

    with two_phase_commit(["primary", "warehouse"]) as txn:
        txn.execute("primary", "INSERT INTO orders ...")
        txn.execute("warehouse", "UPDATE stock SET qty = qty - 1 ...")

    # On clean exit, both COMMIT PREPARED. On exception, both
    # ROLLBACK PREPARED.

PG-only. ``max_prepared_transactions`` must be set on the server
(``postgresql.conf``) before ``PREPARE TRANSACTION`` succeeds.
"""
from __future__ import annotations

import contextlib
import logging
import secrets
from typing import Any

_log = logging.getLogger("dorm.contrib.two_phase")


class TwoPhaseError(Exception):
    """Raised when a 2PC phase fails on at least one participant."""


class _TxnContext:
    """Handle yielded by :func:`two_phase_commit`. Exposes
    ``execute(alias, sql, params)`` so the caller can dispatch work
    to each participant without re-grabbing connections."""

    def __init__(self, aliases: list[str]) -> None:
        self._aliases = aliases

    def execute(
        self, alias: str, sql: str, params: list[Any] | None = None
    ) -> Any:
        if alias not in self._aliases:
            raise KeyError(
                f"two_phase_commit: alias {alias!r} not in "
                f"participants {self._aliases!r}"
            )
        from ..db.connection import get_connection

        conn = get_connection(alias)
        upper = sql.lstrip().upper()
        if upper.startswith(("SELECT", "WITH")):
            return conn.execute(sql, params)
        return conn.execute_write(sql, params)


def _require_pg(alias: str) -> None:
    from ..db.connection import get_connection

    conn = get_connection(alias)
    if getattr(conn, "vendor", None) != "postgresql":
        raise NotImplementedError(
            f"two_phase_commit: alias {alias!r} is not PostgreSQL "
            "— PREPARE TRANSACTION has no portable counterpart."
        )


@contextlib.contextmanager
def two_phase_commit(aliases: list[str]):
    """Coordinate a 2PC across *aliases*.

    On clean exit, every participant runs ``COMMIT PREPARED``.
    On exception, every participant runs ``ROLLBACK PREPARED``.

    Raises :class:`TwoPhaseError` when at least one participant
    fails to prepare — the rest are rolled back and the original
    exception is re-raised.
    """
    if not aliases:
        raise ValueError("two_phase_commit requires at least one alias")
    for a in aliases:
        _require_pg(a)
    # Reject nested atomic: 2PC needs to run BEGIN / PREPARE / COMMIT
    # PREPARED on a connection it owns; an outer atomic block would
    # have already taken the connection out of the pool and pinned it
    # to a different transaction.
    from ..db.connection import get_connection as _gc

    for a in aliases:
        if getattr(_gc(a), "_atomic_conn", None) is not None:
            raise RuntimeError(
                f"two_phase_commit({aliases!r}) cannot run while alias "
                f"{a!r} is inside an atomic() block — the 2PC coordinator "
                "must own the connection's transaction state. Exit the "
                "atomic block first."
            )

    # Global transaction id. PG accepts arbitrary strings (up to 200
    # bytes); the hex token keeps it short and collision-resistant.
    gid_base = secrets.token_hex(16)
    txn = _TxnContext(aliases)

    from ..db.connection import get_connection

    # Open one ``BEGIN`` per participant; they remain open until we
    # decide commit vs rollback.
    for a in aliases:
        conn = get_connection(a)
        conn.execute_script("BEGIN")

    try:
        yield txn
    except Exception:
        # Rollback the in-progress transactions; nothing was prepared
        # yet at this point.
        for a in aliases:
            try:
                get_connection(a).execute_script("ROLLBACK")
            except Exception:  # pragma: no cover
                pass
        raise

    # Phase 1: PREPARE on every participant.
    prepared: list[str] = []
    prepare_errors: list[tuple[str, BaseException]] = []
    for a in aliases:
        gid = f"{gid_base}-{a}"
        try:
            get_connection(a).execute_script(
                f"PREPARE TRANSACTION '{gid}'"
            )
            prepared.append(a)
        except Exception as exc:
            prepare_errors.append((a, exc))
            break

    if prepare_errors:
        # Roll back every successfully-prepared participant.
        for a in prepared:
            gid = f"{gid_base}-{a}"
            try:
                get_connection(a).execute_script(
                    f"ROLLBACK PREPARED '{gid}'"
                )
            except Exception:  # pragma: no cover
                _log.error(
                    "two_phase_commit: ROLLBACK PREPARED failed on %r", a
                )
        # Roll back any in-progress (non-prepared) participants.
        for a in aliases:
            if a in prepared:
                continue
            try:
                get_connection(a).execute_script("ROLLBACK")
            except Exception:  # pragma: no cover
                pass
        alias, err = prepare_errors[0]
        raise TwoPhaseError(
            f"PREPARE TRANSACTION failed on alias {alias!r}: {err}"
        ) from err

    # Phase 2: COMMIT PREPARED everywhere.
    commit_errors: list[tuple[str, BaseException]] = []
    for a in aliases:
        gid = f"{gid_base}-{a}"
        try:
            get_connection(a).execute_script(
                f"COMMIT PREPARED '{gid}'"
            )
        except Exception as exc:
            commit_errors.append((a, exc))
            _log.error(
                "two_phase_commit: COMMIT PREPARED failed on %r — "
                "manual recovery via 'pg_prepared_xacts' required.",
                a,
            )

    if commit_errors:
        alias, err = commit_errors[0]
        raise TwoPhaseError(
            f"COMMIT PREPARED failed on alias {alias!r}: {err}. "
            "Inspect 'pg_prepared_xacts' on the affected node."
        ) from err


__all__ = ["two_phase_commit", "TwoPhaseError"]
