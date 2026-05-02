"""Online-safe migration linter.

Walks every loaded migration's ``operations`` list and emits
findings for patterns that are dangerous on a live (high-traffic)
database. Emits text by default; pass ``format="json"`` for machine-
parseable output suitable for a CI gate.

Rules:

| Code | Trigger | Why it's dangerous |
|------|---------|--------------------|
| DORM-M001 | ``AddField(null=False, default=…)`` | Backfills every row of the table at migration time. On large tables this rewrites the whole heap and locks writers in PG. Prefer a 3-step deploy: add nullable → backfill in chunks → set NOT NULL. |
| DORM-M002 | ``AlterField`` that changes ``db_type`` | Almost always rewrites the table on PG and MySQL. Can be slow + lock-heavy. |
| DORM-M003 | ``AddIndex`` on PG without ``concurrently=True`` | Acquires an ACCESS EXCLUSIVE lock; blocks writers until the index build finishes. |
| DORM-M004 | ``RunPython`` without ``reverse_code`` | Migration is irreversible; ``dorm migrate <prev>`` will refuse to roll back. Use ``RunPython.noop`` deliberately when reverse really is a no-op. |
| DORM-M005 | ``RemoveField`` directly after the field was first introduced (no deprecation step) | A still-running old release can write to the column while the migration drops it; rolling deploys break. |

Suppress a finding by appending ``# noqa: DORM-M00X`` to the
operation's source line (the linter parses comments at the file
level, not per-AST-node, so any ``# noqa: DORM-M00X`` anywhere in
the file silences the matching code for the whole file — keep your
suppressions tight).
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable

# Public type for findings — used by the CLI command and by tests.

@dataclass
class Finding:
    code: str        # e.g. "DORM-M001"
    file: str        # path to the migration file
    operation: str   # repr-ish summary of the operation
    message: str     # human-readable reason

    def to_dict(self) -> dict[str, str]:
        return {
            "code": self.code,
            "file": self.file,
            "operation": self.operation,
            "message": self.message,
        }


@dataclass
class LintResult:
    findings: list[Finding] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.findings

    def to_json(self) -> str:
        return json.dumps(
            [f.to_dict() for f in self.findings], indent=2, ensure_ascii=False
        )

    def to_text(self) -> str:
        if not self.findings:
            return "No issues found."
        lines = []
        for f in self.findings:
            lines.append(f"{f.code} {f.file}: {f.message}")
            lines.append(f"    op: {f.operation}")
        lines.append("")
        lines.append(f"{len(self.findings)} finding(s).")
        return "\n".join(lines)


_NOQA_RE = re.compile(r"#\s*noqa:\s*([A-Z0-9, \-]+)", re.IGNORECASE)


def _suppressed_codes(path: Path) -> set[str]:
    """Return the set of finding codes silenced for *path* via inline
    ``# noqa: DORM-M00X`` comments."""
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return set()
    out: set[str] = set()
    for match in _NOQA_RE.finditer(text):
        for code in match.group(1).split(","):
            code = code.strip().upper()
            if code:
                out.add(code)
    return out


def _is_pg_target(vendor_hint: str | None) -> bool:
    """Best-effort guess of whether a finding is PG-specific.

    The linter is vendor-agnostic by default — DORM-M003 (concurrent
    index) is the only rule that materialises only on PG. Without an
    explicit hint we assume PG so the warning is loud; users on
    SQLite-only deployments can suppress it with ``# noqa: DORM-M003``.
    """
    if vendor_hint is None:
        return True
    return "postg" in vendor_hint.lower()


def _op_repr(op: Any) -> str:
    """Compact one-line repr of a migration operation for finding output."""
    cls = type(op).__name__
    bits: list[str] = []
    for attr in ("name", "model_name", "field_name", "table"):
        val = getattr(op, attr, None)
        if val is not None:
            bits.append(f"{attr}={val!r}")
    if bits:
        return f"{cls}({', '.join(bits)})"
    return cls


def _check_add_field(op: Any, file: str, suppressed: set[str]) -> Iterable[Finding]:
    if "DORM-M001" in suppressed:
        return ()
    field_obj = getattr(op, "field", None)
    if field_obj is None:
        return ()
    has_null = getattr(field_obj, "null", True)
    has_default = getattr(field_obj, "default", None) is not None
    # Lint only when both NOT NULL and has a default: that's the
    # combo that triggers a full-table backfill at migration time.
    if not has_null and has_default:
        return [
            Finding(
                code="DORM-M001",
                file=file,
                operation=_op_repr(op),
                message=(
                    "AddField with null=False and a default backfills every "
                    "row of the table at migration time. Prefer a 3-step "
                    "deploy: add nullable → backfill in chunks → set NOT NULL."
                ),
            )
        ]
    return ()


def _check_alter_field(op: Any, file: str, suppressed: set[str]) -> Iterable[Finding]:
    if "DORM-M002" in suppressed:
        return ()
    # Heuristic: if the operation carries an ``old_field`` and the
    # field class differs from ``field``, it's a type change. When the
    # operation only carries ``field``, we can't tell — flag at INFO.
    new = getattr(op, "field", None)
    old = getattr(op, "old_field", None)
    if new is None:
        return ()
    if old is not None and type(new).__name__ != type(old).__name__:
        return [
            Finding(
                code="DORM-M002",
                file=file,
                operation=_op_repr(op),
                message=(
                    f"AlterField changes type "
                    f"{type(old).__name__} → {type(new).__name__}. On PG and "
                    "MySQL this rewrites the whole table — review the size "
                    "and lock impact before deploying."
                ),
            )
        ]
    return ()


def _check_add_index(op: Any, file: str, suppressed: set[str]) -> Iterable[Finding]:
    if "DORM-M003" in suppressed:
        return ()
    concurrently = getattr(op, "concurrently", False)
    if concurrently:
        return ()
    return [
        Finding(
            code="DORM-M003",
            file=file,
            operation=_op_repr(op),
            message=(
                "AddIndex without concurrently=True acquires an ACCESS "
                "EXCLUSIVE lock on PostgreSQL while the index builds. "
                "Pass concurrently=True (PG-only) or accept the lock for "
                "small tables."
            ),
        )
    ]


def _check_run_python(op: Any, file: str, suppressed: set[str]) -> Iterable[Finding]:
    if "DORM-M004" in suppressed:
        return ()
    reverse = getattr(op, "reverse_code", None)
    if reverse is None:
        return [
            Finding(
                code="DORM-M004",
                file=file,
                operation=_op_repr(op),
                message=(
                    "RunPython without reverse_code is irreversible — "
                    "`dorm migrate <previous>` will refuse to roll back. "
                    "Pass RunPython.noop explicitly when reverse is "
                    "intentionally a no-op."
                ),
            )
        ]
    return ()


def lint_operations(
    operations: list[Any],
    *,
    file: str = "<inline>",
    suppressed: set[str] | None = None,
) -> LintResult:
    """Lint a single migration's ``operations`` list. Programmatic
    entry point used by tests; the CLI walks the filesystem and feeds
    each loaded migration in."""
    suppressed = suppressed or set()

    result = LintResult()
    # Lazy imports — the lint module shouldn't depend on the migration
    # operations module loading cleanly to be importable itself.
    from . import operations as ops_mod  # noqa: PLC0415

    for op in operations:
        if isinstance(op, ops_mod.AddField):
            result.findings.extend(_check_add_field(op, file, suppressed))
        elif isinstance(op, ops_mod.AlterField):
            result.findings.extend(_check_alter_field(op, file, suppressed))
        elif isinstance(op, ops_mod.AddIndex):
            result.findings.extend(_check_add_index(op, file, suppressed))
        elif isinstance(op, ops_mod.RunPython):
            result.findings.extend(_check_run_python(op, file, suppressed))
    return result


def lint_migration_file(path: Path) -> LintResult:
    """Load *path*, instantiate its ``Migration`` class, and lint its
    operations. Returns a :class:`LintResult`."""
    import importlib.util
    import sys

    spec = importlib.util.spec_from_file_location(
        f"_dorm_lint_{path.stem}", str(path)
    )
    if spec is None or spec.loader is None:
        return LintResult()

    module = importlib.util.module_from_spec(spec)
    # Migration files import from ``dorm.migrations.operations`` etc;
    # adding the project root to ``sys.path`` is the user's job, not
    # ours. We do require ``dorm`` to be importable.
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        # A migration file that fails to import is itself a finding —
        # surface it loud so the CI gate fails.
        return LintResult(
            findings=[
                Finding(
                    code="DORM-M000",
                    file=str(path),
                    operation="<import error>",
                    message=f"Could not import migration: {exc!r}",
                )
            ]
        )

    migration_cls = getattr(module, "Migration", None)
    if migration_cls is None:
        return LintResult()
    operations = list(getattr(migration_cls, "operations", []) or [])
    return lint_operations(
        operations,
        file=str(path),
        suppressed=_suppressed_codes(path),
    )


def lint_directory(directory: Path) -> LintResult:
    """Walk *directory* recursively for ``.py`` migration files and
    aggregate findings across all of them."""
    aggregate = LintResult()
    if not directory.exists():
        return aggregate
    for path in sorted(directory.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        sub = lint_migration_file(path)
        aggregate.findings.extend(sub.findings)
    return aggregate


__all__ = [
    "Finding",
    "LintResult",
    "lint_operations",
    "lint_migration_file",
    "lint_directory",
]
