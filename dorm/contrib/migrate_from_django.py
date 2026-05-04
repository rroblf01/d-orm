"""Auto-port Django ``models.py`` files to dorm-shaped equivalents.

Drives the ``dorm migrate-from-django`` CLI command. Reads a target
file (or directory containing ``models.py``), parses it with the
:mod:`ast` module, and emits a dorm-flavoured rewrite. Operates on
text only — no Django install required, no Django settings loaded,
no models imported.

Conversion targets:

- ``from django.db import models`` → ``import dorm``
- ``models.Model`` base class → ``dorm.Model``
- Every field reference (``models.CharField`` etc.) → ``dorm.<Field>``
  for the supported subset (full mapping table below).
- ``Meta`` inner classes are kept verbatim — option names match
  one-to-one (``db_table``, ``ordering``, ``unique_together``,
  ``indexes``, ``constraints``, ``verbose_name``, ``app_label``,
  ``managed``, ``proxy``).
- ``on_delete=models.CASCADE`` (and friends) → ``on_delete=dorm.CASCADE``.
- ``related_name``, ``null``, ``blank``, ``default``, ``db_column``,
  ``unique``, ``db_index`` keyword arguments survive unchanged.

Things that can't be auto-converted are flagged via inline ``# TODO:
dorm migrate-from-django``: custom managers, ``GenericForeignKey``
(handled via ``dorm.contrib.contenttypes``), ``django.contrib.auth``
imports (point at ``dorm.contrib.auth`` instead), Django signals
imports, and any unrecognised field class.

Migration files (``<app>/migrations/0001_initial.py`` etc.) are NOT
auto-ported here — they encode Django's internal operation classes
which don't map 1:1. Re-run ``dorm makemigrations`` against the
converted models to produce a fresh migration history.
"""

from __future__ import annotations

import ast
import re
from pathlib import Path

# Django field-class name → dorm field-class name. The vast majority
# of names are identical; the table mostly carries the renames /
# folded-in additions (``models.JSONField`` lives in dorm at the
# top level, ``models.PositiveIntegerField`` is just ``IntegerField``
# with a check, etc.). Anything not in this dict is preserved
# verbatim with a ``TODO`` flag.
_DJANGO_FIELD_TO_DORM = {
    "AutoField": "AutoField",
    "BigAutoField": "BigAutoField",
    "SmallAutoField": "SmallAutoField",
    "BigIntegerField": "BigIntegerField",
    "BinaryField": "BinaryField",
    "BooleanField": "BooleanField",
    "CharField": "CharField",
    "DateField": "DateField",
    "DateTimeField": "DateTimeField",
    "DecimalField": "DecimalField",
    "DurationField": "DurationField",
    "EmailField": "EmailField",
    "FileField": "FileField",
    "FilePathField": "FilePathField",
    "FloatField": "FloatField",
    "ForeignKey": "ForeignKey",
    "GeneratedField": "GeneratedField",
    "GenericIPAddressField": "GenericIPAddressField",
    "ImageField": "ImageField",
    "IntegerField": "IntegerField",
    "JSONField": "JSONField",
    "ManyToManyField": "ManyToManyField",
    "OneToOneField": "OneToOneField",
    "PositiveBigIntegerField": "PositiveBigIntegerField",
    "PositiveIntegerField": "PositiveIntegerField",
    "PositiveSmallIntegerField": "PositiveSmallIntegerField",
    "SlugField": "SlugField",
    "SmallIntegerField": "SmallIntegerField",
    "TextField": "TextField",
    "TimeField": "TimeField",
    "URLField": "URLField",
    "UUIDField": "UUIDField",
}

# Django contrib.postgres types — point users at the dorm
# equivalents, but flag as TODO since the constructor shape may
# differ on edge cases.
_DJANGO_PG_FIELD_TO_DORM = {
    "ArrayField": ("ArrayField", "PostgreSQL only — unchanged in dorm."),
    "RangeField": ("RangeField", "PostgreSQL range types."),
    "IntegerRangeField": ("IntegerRangeField", "PostgreSQL range types."),
    "BigIntegerRangeField": ("BigIntegerRangeField", "PostgreSQL range types."),
    "DecimalRangeField": ("DecimalRangeField", "PostgreSQL range types."),
    "DateRangeField": ("DateRangeField", "PostgreSQL range types."),
    "DateTimeRangeField": ("DateTimeRangeField", "PostgreSQL range types."),
}

# CASCADE / SET_NULL / PROTECT / etc. — the constants live on
# ``models.`` in Django and on ``dorm.`` in dorm. Same names.
_ON_DELETE_CONSTANTS = frozenset({
    "CASCADE",
    "SET_NULL",
    "SET_DEFAULT",
    "PROTECT",
    "DO_NOTHING",
    "RESTRICT",
})


def convert_models_source(source: str) -> tuple[str, list[str]]:
    """Convert a Django ``models.py`` source string to a dorm-flavoured
    equivalent. Returns ``(rewritten_source, todo_comments)`` — the
    list captures every conversion the tool couldn't make
    automatically so the caller can surface them to the user."""
    todos: list[str] = []

    # The conversion is line / regex driven rather than full AST
    # rewrite so it preserves the user's formatting + comments.
    # We do parse the source once (read-only) to detect class-level
    # constructs that need flagging — TODOs land at the top of the
    # rewritten file.
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise ValueError(
            f"Could not parse models.py — syntax error at line "
            f"{exc.lineno}: {exc.msg}"
        ) from exc

    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef):
            for stmt in node.body:
                if isinstance(stmt, ast.Assign) and len(stmt.targets) == 1:
                    target = stmt.targets[0]
                    if isinstance(target, ast.Name) and target.id == "objects":
                        todos.append(
                            f"Class {node.name!r}: custom Manager — port to "
                            f"``dorm.Manager`` subclass; the API is identical "
                            f"but ``get_queryset`` returns ``dorm.QuerySet``."
                        )
        if isinstance(node, ast.ImportFrom):
            mod = node.module or ""
            if mod.startswith("django.contrib.auth"):
                todos.append(
                    f"``from {mod} import …`` — replace with "
                    f"``from dorm.contrib.auth import …`` (User / Group / "
                    f"Permission have the same shape)."
                )
            elif mod.startswith("django.contrib.contenttypes"):
                todos.append(
                    f"``from {mod} import …`` — replace with "
                    f"``from dorm.contrib.contenttypes import …`` "
                    f"(GenericForeignKey / GenericRelation parity)."
                )
            elif mod == "django.db.models.signals":
                todos.append(
                    "``from django.db.models.signals import …`` — replace "
                    "with ``from dorm.signals import …``. Async receivers "
                    "are supported (``async def`` works for every signal)."
                )

    out = source

    # ── Imports ───────────────────────────────────────────────────────────
    out = re.sub(
        r"^from\s+django\.db\s+import\s+models\s*$",
        "import dorm",
        out,
        flags=re.MULTILINE,
    )
    out = re.sub(
        r"^from\s+django\.db\s+import\s+models,\s*",
        "import dorm\nfrom django.db import ",
        out,
        flags=re.MULTILINE,
    )
    out = re.sub(
        r"^import\s+django\.db\.models\s*$",
        "import dorm",
        out,
        flags=re.MULTILINE,
    )

    # ── Model base class ──────────────────────────────────────────────────
    out = re.sub(r"\bmodels\.Model\b", "dorm.Model", out)

    # ── Field references ─────────────────────────────────────────────────
    # First the contrib.postgres redirect — emit a TODO note alongside.
    for django_name, (dorm_name, note) in _DJANGO_PG_FIELD_TO_DORM.items():
        if re.search(rf"\bmodels\.{django_name}\b", out) or re.search(
            rf"\bpostgres\.{django_name}\b", out
        ):
            todos.append(f"``{django_name}`` → ``dorm.{dorm_name}``: {note}")
        out = re.sub(
            rf"\b(?:models|postgres)\.{django_name}\b",
            f"dorm.{dorm_name}",
            out,
        )

    # Then the regular field map.
    for django_name, dorm_name in _DJANGO_FIELD_TO_DORM.items():
        out = re.sub(
            rf"\bmodels\.{django_name}\b",
            f"dorm.{dorm_name}",
            out,
        )

    # ── on_delete constants ───────────────────────────────────────────────
    for const in _ON_DELETE_CONSTANTS:
        out = re.sub(rf"\bmodels\.{const}\b", f"dorm.{const}", out)

    # ── Index / constraint / Q references ─────────────────────────────────
    out = re.sub(r"\bmodels\.Index\b", "dorm.Index", out)
    out = re.sub(r"\bmodels\.UniqueConstraint\b", "dorm.UniqueConstraint", out)
    out = re.sub(r"\bmodels\.CheckConstraint\b", "dorm.CheckConstraint", out)
    out = re.sub(r"\bmodels\.Q\b", "dorm.Q", out)
    out = re.sub(r"\bmodels\.F\b", "dorm.F", out)
    out = re.sub(r"\bmodels\.Value\b", "dorm.Value", out)

    # ── Aggregates / expressions namespace ───────────────────────────────
    # Django imports aggregates as ``from django.db.models import Count, Sum``;
    # dorm has them at the top level, so a wholesale ``django.db.models`` →
    # ``dorm`` rewrite isn't safe (would clobber the ``models`` we just
    # remapped). Stop at the explicit names users tend to import.
    out = re.sub(
        r"^from\s+django\.db\.models\s+import\s+",
        "from dorm import ",
        out,
        flags=re.MULTILINE,
    )

    # ── Lingering ``models.<X>`` references — flag for review ────────────
    leftovers = sorted(set(re.findall(r"\bmodels\.([A-Za-z_][A-Za-z0-9_]*)", out)))
    for ref in leftovers:
        todos.append(
            f"Unrecognised reference ``models.{ref}`` left in the output — "
            f"check whether dorm has an equivalent and update by hand."
        )

    if todos:
        banner = (
            "# TODO: dorm migrate-from-django flagged the following items.\n"
            "# Review each before running tests against the converted file:\n"
        )
        banner += "".join(f"#   - {line}\n" for line in todos)
        out = banner + "\n" + out

    return out, todos


def convert_models_file(path: Path) -> tuple[str, list[str]]:
    """Read *path* (typically ``<app>/models.py``), convert its
    contents, and return the rewritten source + the TODO list. The
    file on disk is **not** modified — call ``write_text`` on the
    return value."""
    if not path.is_file():
        raise FileNotFoundError(f"models.py not found at {path}")
    source = path.read_text(encoding="utf-8")
    return convert_models_source(source)


def convert_app(app_dir: Path, *, dry_run: bool = False) -> dict[str, list[str]]:
    """Walk *app_dir* (a Django app folder), convert ``models.py``
    in-place, and return a ``{filename: [todo, …]}`` dict.

    With ``dry_run=True`` the converted output is computed but the
    files on disk are not modified — useful for the CLI's preview
    flow.
    """
    if not app_dir.is_dir():
        raise FileNotFoundError(f"App directory not found: {app_dir}")
    out: dict[str, list[str]] = {}
    targets = []
    models_py = app_dir / "models.py"
    if models_py.is_file():
        targets.append(models_py)
    # Sub-package layout: ``<app>/models/__init__.py``, ``<app>/models/foo.py``.
    models_pkg = app_dir / "models"
    if models_pkg.is_dir():
        targets.extend(
            p for p in models_pkg.glob("**/*.py") if p.name != "__init__.py"
        )
    if not targets:
        raise FileNotFoundError(
            f"No models.py / models/ package found under {app_dir}"
        )

    for target in targets:
        rewritten, todos = convert_models_file(target)
        out[str(target)] = todos
        if not dry_run:
            target.write_text(rewritten, encoding="utf-8")
    return out


__all__ = [
    "convert_models_source",
    "convert_models_file",
    "convert_app",
]
