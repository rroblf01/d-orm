"""SQL allow-list (CSP-style) for hardened production deployments.

Activate with :func:`install` to register a process-wide allow-list
that rejects any query whose **template** (literals stripped) isn't
on the approved list. Use during the canary phase of a new release:
capture every emitted template via :mod:`dorm.contrib.querystats`,
commit the curated list to source control, then enable enforcement
so a runtime regression (or an SQL-injection that re-shapes the query
parse tree) is blocked before reaching the database.

Caveats:

- The allow-list is opt-in and process-local. Turning it on in
  production after release without a capture phase will reject
  legitimate traffic.
- DDL emitted by migrations bypasses the gate by default — pass
  ``allow_ddl=False`` to enforce on every statement.
- Cache hits / queries from sibling tools (e.g. ``dorm shell``)
  flow through the same hook; add their templates or unset the
  gate while debugging.
"""
from __future__ import annotations

import logging
import re
import threading
from dataclasses import dataclass, field
from typing import Any

from .. import signals

_log = logging.getLogger("dorm.contrib.sql_allowlist")

_STRIP_PATTERNS = [
    (re.compile(r"\b\d+\b"), "?"),
    (re.compile(r"'[^']*'"), "?"),
    (re.compile(r'"[^"]*"'), '"?"'),
    (re.compile(r"\$\d+"), "?"),
    (re.compile(r"%s"), "?"),
]

_DDL_PREFIXES = (
    "CREATE", "DROP", "ALTER", "TRUNCATE", "GRANT", "REVOKE", "COMMENT",
    "BEGIN", "COMMIT", "ROLLBACK", "SAVEPOINT", "RELEASE", "VACUUM",
    "ANALYZE", "REINDEX", "SET", "EXPLAIN",
)


def _template(sql: str) -> str:
    out = sql
    for pat, repl in _STRIP_PATTERNS:
        out = pat.sub(repl, out)
    out = re.sub(r"\s+", " ", out).strip()
    return out


@dataclass
class _AllowState:
    enabled: bool = False
    templates: set[str] = field(default_factory=set)
    allow_ddl: bool = True
    raise_on_violation: bool = True
    rejected: list[str] = field(default_factory=list)
    _dispatch_uid: str = "dorm-sql-allowlist"


_state = _AllowState()
_lock = threading.Lock()


class SQLNotAllowedError(Exception):
    """Raised when the allow-list rejects a query template."""


def _is_ddl(sql: str) -> bool:
    upper = sql.lstrip().upper()
    return upper.startswith(_DDL_PREFIXES)


def _on_pre_query(sender: Any, **kwargs: Any) -> None:
    sql = str(kwargs.get("sql", ""))
    if not sql:
        return
    with _lock:
        st = _state
        enabled = st.enabled
        templates = st.templates
        allow_ddl = st.allow_ddl
        raise_on = st.raise_on_violation
    if not enabled:
        return
    if allow_ddl and _is_ddl(sql):
        return
    tpl = _template(sql)
    if tpl in templates:
        return
    with _lock:
        st.rejected.append(tpl)
    msg = f"SQL allow-list rejected template: {tpl}"
    if raise_on:
        raise SQLNotAllowedError(msg)
    _log.warning(msg)


_prev_raise_exceptions: bool | None = None


def install(
    templates: list[str],
    *,
    allow_ddl: bool = True,
    raise_on_violation: bool = True,
) -> None:
    """Enable allow-list enforcement with *templates*.

    Each entry is run through the same literal-stripping
    normalisation as the runtime query, so callers can paste in the
    raw SQL fragment without worrying about the placeholder shape.
    Repeat calls replace the previous allow-list.

    Side-effect: flips ``signals.pre_query.raise_exceptions = True``
    so a rejected template aborts the query before it reaches the
    cursor. :func:`uninstall` restores the previous value.
    """
    global _prev_raise_exceptions
    with _lock:
        _state.enabled = True
        _state.templates = {_template(t) for t in templates}
        _state.allow_ddl = allow_ddl
        _state.raise_on_violation = raise_on_violation
        _state.rejected.clear()
    try:
        signals.pre_query.disconnect(dispatch_uid=_state._dispatch_uid)
    except Exception:  # pragma: no cover
        pass
    signals.pre_query.connect(
        _on_pre_query, weak=False, dispatch_uid=_state._dispatch_uid
    )
    if raise_on_violation:
        _prev_raise_exceptions = signals.pre_query.raise_exceptions
        signals.pre_query.raise_exceptions = True


def uninstall() -> None:
    """Disable the allow-list and forget every recorded violation."""
    global _prev_raise_exceptions
    with _lock:
        _state.enabled = False
        _state.templates.clear()
        _state.rejected.clear()
    try:
        signals.pre_query.disconnect(dispatch_uid=_state._dispatch_uid)
    except Exception:  # pragma: no cover
        pass
    if _prev_raise_exceptions is not None:
        signals.pre_query.raise_exceptions = _prev_raise_exceptions
        _prev_raise_exceptions = None


def rejected_templates() -> list[str]:
    """Return the list of templates rejected since install. Use to
    surface gaps during the canary phase before flipping
    ``raise_on_violation=True``."""
    with _lock:
        return list(_state.rejected)


def allowed_templates() -> list[str]:
    """Snapshot of the currently-installed allow-list — useful for
    diff-ing against ``rejected_templates()`` in the canary phase."""
    with _lock:
        return sorted(_state.templates)


def dump_captured(path: str, *, include_allowed: bool = True) -> str:
    """Write the current allow-list (and rejected templates) to *path*
    as a JSON document.

    The schema is::

        {
            "allowed": ["SELECT ? FROM users", ...],
            "rejected": ["DELETE FROM users WHERE id = ?", ...]
        }

    Callers typically use this during a canary phase: run traffic
    against ``install([...], raise_on_violation=False)``, dump the
    union of ``allowed`` + ``rejected`` to disk, prune by hand, then
    feed the curated file back in via :func:`load_from_file` for the
    enforcement phase.
    """
    import json

    with _lock:
        payload = {
            "rejected": list(_state.rejected),
        }
        if include_allowed:
            payload["allowed"] = sorted(_state.templates)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    return path


def load_from_file(
    path: str,
    *,
    allow_ddl: bool = False,
    raise_on_violation: bool = True,
    field: str = "allowed",
) -> int:
    """Install the allow-list from a JSON file (the same shape
    :func:`dump_captured` writes). Returns the number of templates
    loaded.

    *field* selects which key carries the templates ('allowed' by
    default; pass 'rejected' to load the captured violations
    verbatim, e.g. for re-auditing).
    """
    import json

    with open(path, "r", encoding="utf-8") as fh:
        payload = json.load(fh)
    templates = list(payload.get(field, []))
    install(
        templates,
        allow_ddl=allow_ddl,
        raise_on_violation=raise_on_violation,
    )
    return len(templates)


__all__ = [
    "install",
    "uninstall",
    "rejected_templates",
    "allowed_templates",
    "dump_captured",
    "load_from_file",
    "SQLNotAllowedError",
]
