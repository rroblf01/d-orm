"""Query-plan drift detection.

Records a baseline ``EXPLAIN`` plan for a SQL statement (typically
captured during load testing or as part of a release script), and
compares fresh plans against it on demand. When the plan changes —
new node type, different join order, scan-vs-index swap — the helper
returns the diff so an operator can decide whether the change is a
performance win or an incoming incident.

Workflow::

    from dorm.contrib.plan_drift import (
        record_baseline,
        compare,
        diff_text,
    )

    sql = "SELECT * FROM orders WHERE customer_id = %s"
    record_baseline("orders.by_customer", sql, params=[1])  # capture

    # Later — in a healthcheck endpoint or cron job:
    result = compare("orders.by_customer", sql, params=[1])
    if result.drifted:
        log.warning("plan drift on %s:\n%s",
                    result.tag, diff_text(result))

The baseline is normalised (cost estimates, row estimates, buffer
counts stripped) so comparisons stay stable across data growth — only
*structural* changes (node types, scan strategies) trip the alarm.
"""
from __future__ import annotations

import re
import threading
from dataclasses import dataclass
from typing import Any

# Strip volatile bits of the EXPLAIN output that change run-to-run
# without indicating a real plan change. PG emits ``(cost=…)``,
# ``rows=N``, ``actual time=…``, ``Buffers: …``, ``Planning Time``,
# ``Execution Time``, ``Memory:``. SQLite's EXPLAIN QUERY PLAN is
# already mostly structural; keep the cleaning conservative for
# that vendor.
_PG_VOLATILE_RE = re.compile(
    r"(?:\s+\(cost=[^)]+\)|"
    r"\s+rows=\d+|"
    r"\s+actual time=[\d.]+\.\.[\d.]+|"
    r"\s+loops=\d+|"
    r"\s+Buffers:[^\n]*|"
    r"\s+Memory:[^\n]*|"
    r"\s+Planning Time:[^\n]*|"
    r"\s+Execution Time:[^\n]*|"
    r"\s+width=\d+)",
    re.IGNORECASE,
)


def _strip_volatile(plan: str) -> str:
    """Return a comparison-friendly version of *plan* with run-time
    metrics removed. Whitespace at line ends is also normalised so
    cosmetic indentation differences don't trip the diff."""
    out = _PG_VOLATILE_RE.sub("", plan)
    return "\n".join(line.rstrip() for line in out.splitlines())


@dataclass(frozen=True)
class CompareResult:
    tag: str
    baseline: str
    current: str
    drifted: bool


_BASELINES: dict[str, str] = {}
_lock = threading.Lock()


def _capture_plan(sql: str, params: list[Any] | None, using: str) -> str:
    from ..db.connection import get_connection

    conn = get_connection(using)
    vendor = getattr(conn, "vendor", "sqlite")
    if vendor == "postgresql":
        plan_sql = f"EXPLAIN (FORMAT TEXT) {sql}"
    elif vendor in ("sqlite", "libsql"):
        plan_sql = f"EXPLAIN QUERY PLAN {sql}"
    elif vendor in ("mysql", "mariadb", "duckdb"):
        plan_sql = f"EXPLAIN {sql}"
    else:
        raise NotImplementedError(
            f"plan_drift: vendor {vendor!r} has no portable EXPLAIN form."
        )
    rows = conn.execute(plan_sql, params or [])
    lines: list[str] = []
    if isinstance(rows, list):
        for r in rows:
            # Accept both ``dict`` and ``sqlite3.Row``-style mappings
            # (they expose ``keys()`` but aren't ``dict`` subclasses).
            if isinstance(r, dict):
                values = r.values()
            elif hasattr(r, "keys") and not isinstance(r, str):
                try:
                    values = [r[k] for k in r.keys()]
                except Exception:
                    values = [str(r)]
            else:
                lines.append(str(r))
                continue
            lines.append(
                " ".join("" if v is None else str(v) for v in values)
            )
    else:
        lines.append(str(rows))
    return "\n".join(lines)


def record_baseline(
    tag: str,
    sql: str,
    *,
    params: list[Any] | None = None,
    using: str = "default",
) -> str:
    """Capture and store the EXPLAIN plan for *sql* under *tag*.

    Subsequent :func:`compare` calls reference this baseline. Re-calling
    :func:`record_baseline` with the same *tag* overwrites the previous
    capture — use that to refresh after a planned schema/index change.
    """
    plan = _capture_plan(sql, params, using)
    normalised = _strip_volatile(plan)
    with _lock:
        _BASELINES[tag] = normalised
    return normalised


def compare(
    tag: str,
    sql: str,
    *,
    params: list[Any] | None = None,
    using: str = "default",
) -> CompareResult:
    """Re-run the EXPLAIN for *sql* and compare against the stored
    baseline for *tag*. Returns a :class:`CompareResult` whose
    ``drifted`` flag is True when the cleaned plans differ.

    Raises :class:`KeyError` when *tag* hasn't been baselined yet.
    """
    with _lock:
        baseline = _BASELINES.get(tag)
    if baseline is None:
        raise KeyError(f"no baseline recorded for tag {tag!r}")
    current = _strip_volatile(_capture_plan(sql, params, using))
    return CompareResult(
        tag=tag,
        baseline=baseline,
        current=current,
        drifted=baseline != current,
    )


def diff_text(result: CompareResult) -> str:
    """Return a unified diff of *result* — ready for log output."""
    import difflib

    return "\n".join(
        difflib.unified_diff(
            result.baseline.splitlines(),
            result.current.splitlines(),
            fromfile=f"baseline:{result.tag}",
            tofile=f"current:{result.tag}",
            lineterm="",
        )
    )


def baselines() -> dict[str, str]:
    """Return a copy of every recorded baseline (tag → cleaned plan)."""
    with _lock:
        return dict(_BASELINES)


def reset() -> None:
    """Drop every recorded baseline."""
    with _lock:
        _BASELINES.clear()


__all__ = [
    "record_baseline",
    "compare",
    "diff_text",
    "baselines",
    "reset",
    "CompareResult",
]
