# `dorm.migrations.lint`

Online-safe migration linter — walks every migration's
`operations` list and emits findings for patterns that are
dangerous on a live (high-traffic) database.

## CLI

```bash
dorm lint-migrations                            # exit non-zero on findings
dorm lint-migrations --format json              # CI-consumable
dorm lint-migrations --rule DORM-M001           # filter (may repeat)
dorm lint-migrations --exit-zero                # advisory mode
```

See the [Migration safety](../production.md#migration-safety-dorm-lint-migrations)
section in the production guide for the full rule table + suppression
syntax.

## Programmatic API

```python
from pathlib import Path
from dorm.migrations.lint import lint_directory

result = lint_directory(Path("myapp/migrations"))
if not result.ok:
    for f in result.findings:
        print(f.code, f.file, f.message)
```

::: dorm.migrations.lint.lint_directory
::: dorm.migrations.lint.lint_migration_file
::: dorm.migrations.lint.lint_operations
::: dorm.migrations.lint.Finding
::: dorm.migrations.lint.LintResult

## Rules

| Code        | Trigger                                          |
|-------------|--------------------------------------------------|
| `DORM-M000` | Migration file failed to import (parse error).   |
| `DORM-M001` | `AddField(null=False, default=…)` — full-table backfill at migrate time. |
| `DORM-M002` | `AlterField` — review whether it changes the type (table rewrite on PG / MySQL) or just toggles NOT NULL / default. |
| `DORM-M003` | `AddIndex` without `concurrently=True` (PG) — ACCESS EXCLUSIVE lock. |
| `DORM-M004` | `RunPython` without `reverse_code` — irreversible. |

Suppress per-file with a `# noqa: DORM-M00X` comment anywhere in
the file.
