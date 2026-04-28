"""Reverse-engineer dorm models from an existing database.

Used by ``dorm inspectdb`` to print Python class definitions for every
table in the connected database, so users adopting dorm in a project
with a pre-existing schema don't have to type out the full model
declarations by hand.

Mapping is best-effort:

- Every column's ``data_type`` is matched against a curated table that
  covers PostgreSQL and SQLite native types.
- Foreign keys are detected via ``information_schema`` on PG and via
  ``PRAGMA foreign_key_list`` on SQLite.
- Constraints / indexes are *not* introspected — the goal is a
  correct first draft of ``models.py`` that the user then commits and
  iterates on with the normal ``makemigrations`` flow.

The output is plain Python source so the user can pipe it into a file::

    dorm inspectdb > legacy_app/models.py
"""
from __future__ import annotations

import re
from typing import Any


# Map data_type → (FieldClass, extra kwargs)
#
# Order is irrelevant — this is a dict lookup. The 2.2 additions
# (``DurationField``, ``CITextField``, the range family) match the
# native PostgreSQL types so a project adopting dorm against an
# existing schema gets sensible field classes instead of
# ``TextField`` placeholders.
_PG_TYPE_MAP: dict[str, tuple[str, dict[str, Any]]] = {
    "integer": ("IntegerField", {}),
    "smallint": ("SmallIntegerField", {}),
    "bigint": ("BigIntegerField", {}),
    "real": ("FloatField", {}),
    "double precision": ("FloatField", {}),
    "numeric": ("DecimalField", {}),
    "boolean": ("BooleanField", {}),
    "text": ("TextField", {}),
    "character varying": ("CharField", {}),
    "varchar": ("CharField", {}),
    "char": ("CharField", {}),
    "date": ("DateField", {}),
    "time": ("TimeField", {}),
    "time without time zone": ("TimeField", {}),
    "timestamp": ("DateTimeField", {}),
    "timestamp without time zone": ("DateTimeField", {}),
    "timestamp with time zone": ("DateTimeField", {}),
    "interval": ("DurationField", {}),
    "json": ("JSONField", {}),
    "jsonb": ("JSONField", {}),
    "uuid": ("UUIDField", {}),
    "bytea": ("BinaryField", {}),
    "inet": ("GenericIPAddressField", {}),
    "citext": ("CITextField", {}),
    # Range types — psycopg surfaces them with these exact ``data_type``
    # strings. Each maps to the corresponding ``RangeField`` subclass.
    "int4range": ("IntegerRangeField", {}),
    "int8range": ("BigIntegerRangeField", {}),
    "numrange": ("DecimalRangeField", {}),
    "daterange": ("DateRangeField", {}),
    "tsrange": ("DateTimeRangeField", {}),
    "tstzrange": ("DateTimeRangeField", {}),
}


_SQLITE_TYPE_MAP: dict[str, tuple[str, dict[str, Any]]] = {
    "INTEGER": ("IntegerField", {}),
    "REAL": ("FloatField", {}),
    "TEXT": ("TextField", {}),
    "BLOB": ("BinaryField", {}),
    "NUMERIC": ("DecimalField", {}),
    "BOOLEAN": ("BooleanField", {}),
    "DATETIME": ("DateTimeField", {}),
    "DATE": ("DateField", {}),
    "TIME": ("TimeField", {}),
}


# CHARACTER VARYING(123) / VARCHAR(123) → ("CharField", {max_length: 123})
_LENGTHED_RE = re.compile(
    r"^(character varying|varchar|char)\s*\(\s*(\d+)\s*\)\s*$",
    re.IGNORECASE,
)
_NUMERIC_RE = re.compile(
    r"^(numeric|decimal)\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)\s*$",
    re.IGNORECASE,
)


def _to_class_name(table: str) -> str:
    """``users_profile`` → ``UsersProfile``. Strips a leading
    ``app_`` prefix when it matches a Pythonic identifier and is
    immediately followed by a meaningful name."""
    parts = re.split(r"[^A-Za-z0-9]+", table)
    return "".join(p.capitalize() for p in parts if p) or "Table"


def _map_type(data_type: str, vendor: str) -> tuple[str, dict[str, Any]]:
    dt = (data_type or "").strip().lower()

    m = _LENGTHED_RE.match(dt)
    if m:
        return "CharField", {"max_length": int(m.group(2))}

    m = _NUMERIC_RE.match(dt)
    if m:
        return "DecimalField", {
            "max_digits": int(m.group(2)),
            "decimal_places": int(m.group(3)),
        }

    if vendor == "postgresql":
        if dt in _PG_TYPE_MAP:
            return _PG_TYPE_MAP[dt][0], dict(_PG_TYPE_MAP[dt][1])
    else:
        # SQLite stores the declared type verbatim and is loose about it.
        for prefix, (cls, kwargs) in _SQLITE_TYPE_MAP.items():
            if dt.upper().startswith(prefix):
                return cls, dict(kwargs)

    # Fallback: unknown type → TextField with a reminder comment.
    return "TextField", {"_inspect_unknown": dt}


def introspect_tables(connection: Any) -> list[dict]:
    """Return a list of ``{name, columns}`` dicts for every user-facing
    table. Migration metadata tables (``dorm_migrations``) and SQLite's
    ``sqlite_*`` system tables are skipped.
    """
    vendor = getattr(connection, "vendor", "sqlite")
    tables: list[str] = []
    if vendor == "postgresql":
        # psycopg parses ``%`` as a placeholder marker; escape with ``%%``
        # so the literal ``LIKE 'pg_%'`` clause survives the rewrite.
        rows = connection.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
            "AND tablename NOT LIKE 'pg_%%' "
            "ORDER BY tablename"
        )
        tables = [r["tablename"] for r in rows]
    else:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        )
        tables = [r["name"] for r in rows]

    skip = {"dorm_migrations"}
    out = []
    for t in tables:
        if t in skip:
            continue
        cols = connection.get_table_columns(t)
        # Foreign keys
        fks: dict[str, str] = {}
        if vendor == "postgresql":
            try:
                fk_rows = connection.execute(
                    """
                    SELECT
                        kcu.column_name AS col,
                        ccu.table_name AS ref_table
                    FROM information_schema.table_constraints tc
                    JOIN information_schema.key_column_usage kcu
                      ON tc.constraint_name = kcu.constraint_name
                    JOIN information_schema.constraint_column_usage ccu
                      ON ccu.constraint_name = tc.constraint_name
                    WHERE tc.constraint_type = 'FOREIGN KEY'
                      AND tc.table_name = %s
                    """,
                    [t],
                )
                for r in fk_rows:
                    fks[r["col"]] = r["ref_table"]
            except Exception:
                pass
        else:
            try:
                fk_rows = connection.execute(f'PRAGMA foreign_key_list("{t}")')
                for r in fk_rows:
                    d = dict(r)
                    src = d.get("from")
                    tgt = d.get("table")
                    if isinstance(src, str) and isinstance(tgt, str):
                        fks[src] = tgt
            except Exception:
                pass

        out.append({"name": t, "columns": cols, "fks": fks, "vendor": vendor})
    return out


def render_models(tables: list[dict]) -> str:
    """Render Python source for the introspected *tables*.

    The output looks like::

        # Auto-generated by ``dorm inspectdb``. Edit and commit.
        import dorm

        class Article(dorm.Model):
            title = dorm.CharField(max_length=200)
            ...
            class Meta:
                db_table = "article"
    """
    lines: list[str] = [
        "# Auto-generated by ``dorm inspectdb``.",
        "# Review the field types — the introspector cannot recover every",
        "# detail (max_length, choices, validators, on_delete behaviour).",
        "import dorm",
        "",
        "",
    ]
    for t in tables:
        cls_name = _to_class_name(t["name"])
        lines.append(f"class {cls_name}(dorm.Model):")
        body_lines = []
        had_pk = False
        for col in t["columns"]:
            cname = col.get("name") or col.get("column_name")
            ctype = (
                col.get("data_type")
                or col.get("type")
                or col.get("DATA_TYPE")
                or ""
            )
            nullable = str(
                col.get("is_nullable") or col.get("notnull") or ""
            ).lower() in {"yes", "true", "1", "0"}  # pragma flag is "notnull": 0/1
            # ``notnull`` semantics: 1 = NOT NULL, 0 = nullable. The
            # str()→lower()→check above is wrong for SQLite. Recompute
            # cleanly:
            if "notnull" in col:
                nullable = not bool(col.get("notnull"))
            elif "is_nullable" in col:
                nullable = str(col["is_nullable"]).upper() == "YES"

            # PK detection: SQLite ``pk`` flag, PG primary-key constraint.
            is_pk = bool(col.get("pk")) or col.get("is_pk") is True
            if cname is None:
                continue

            # FK?
            if cname in t["fks"] and cname.endswith("_id"):
                ref = t["fks"][cname]
                attr = cname[:-3]
                body_lines.append(
                    f"    {attr} = dorm.ForeignKey("
                    f"'{_to_class_name(ref)}', on_delete=dorm.CASCADE"
                    + (", null=True" if nullable else "")
                    + ")"
                )
                continue

            cls, kwargs = _map_type(ctype, t["vendor"])
            kwarg_parts: list[str] = []
            for k, v in kwargs.items():
                if k.startswith("_"):
                    continue
                kwarg_parts.append(f"{k}={v!r}")
            if is_pk:
                kwarg_parts.append("primary_key=True")
                had_pk = True
            if nullable and not is_pk:
                kwarg_parts.append("null=True")
            joined = ", ".join(kwarg_parts)
            body_lines.append(f"    {cname} = dorm.{cls}({joined})")
            if "_inspect_unknown" in kwargs:
                body_lines.append(
                    f"    # NOTE: column {cname!r} had unrecognised "
                    f"type {kwargs['_inspect_unknown']!r}; defaulted to TextField."
                )
        if not body_lines:
            body_lines.append("    pass")
        lines.extend(body_lines)
        lines.append("")
        lines.append("    class Meta:")
        lines.append(f"        db_table = {t['name']!r}")
        if not had_pk:
            lines.append(
                "        # NOTE: introspector did not find an explicit PK; "
                "dorm will inject a BigAutoField named 'id' if no field is "
                "marked primary_key=True."
            )
        lines.append("")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
