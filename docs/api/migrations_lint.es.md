# `dorm.migrations.lint`

Linter de migraciones online-safe — recorre cada lista
`operations` de migración y emite hallazgos para patrones
peligrosos en BD viva (alto tráfico).

## CLI

```bash
dorm lint-migrations                            # exit != 0 si hay hallazgos
dorm lint-migrations --format json              # consumible por CI
dorm lint-migrations --rule DORM-M001           # filtrar (puede repetir)
dorm lint-migrations --exit-zero                # modo advisory
```

Ver la sección [Seguridad de migraciones](../production.md#seguridad-de-migraciones-dorm-lint-migrations)
en la guía de producción para tabla completa de reglas + sintaxis de
supresión.

## API programática

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

## Reglas

| Código      | Trigger                                          |
|-------------|--------------------------------------------------|
| `DORM-M000` | Archivo de migración falló al importar (parse error). |
| `DORM-M001` | `AddField(null=False, default=…)` — backfill de tabla completa en migrate time. |
| `DORM-M002` | `AlterField` — revisa si cambia el tipo (reescritura tabla en PG / MySQL) o solo toggle NOT NULL / default. |
| `DORM-M003` | `AddIndex` sin `concurrently=True` (PG) — lock ACCESS EXCLUSIVE. |
| `DORM-M004` | `RunPython` sin `reverse_code` — irreversible. |

Silencia per-fichero con un comentario `# noqa: DORM-M00X` en
cualquier sitio del archivo.
