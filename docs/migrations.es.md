# Migraciones

El sistema de migraciones de dorm sigue el mismo patrón que Django:
cada migración es un archivo Python con una lista de objetos
`Operation` que describe un único paso forward. El autodetector
compara el estado de tus modelos con la última migración y escribe el
diff por ti.

## El bucle del día a día

```bash
# 1. Edita tus modelos
# 2. Genera una migración
dorm makemigrations

# 3. Revisa el SQL que emitiría (opcional pero recomendado)
dorm migrate --dry-run

# 4. Aplica
dorm migrate
```

Cada archivo de migración vive en `<app>/migrations/000N_<nombre>.py`
y se aplica en orden. dorm registra las migraciones aplicadas en una
tabla `dorm_migrations` dentro de tu base de datos, así que volver a
ejecutar `migrate` siempre es seguro.

## Qué detecta `makemigrations`

- Modelos nuevos / eliminados → `CreateModel` / `DeleteModel`
- Columnas nuevas / eliminadas → `AddField` / `RemoveField`
- Cambios de opciones de campo (max_length, null, default, ...) →
  `AlterField`
- Modelos / campos renombrados → `RenameModel` / `RenameField`
  (pide confirmación cuando un remove-then-add es ambiguo)
- `Meta.indexes` nuevos / eliminados → `AddIndex` / `RemoveIndex`

El detector corre en Python puro sobre el registro `_meta` de los
modelos — no necesita tocar la BD.

## Migraciones vacías para data migrations

```bash
dorm makemigrations --empty --name backfill_slugs blog
```

Genera un stub con `RunPython` y `RunSQL` para que lo rellenes:

```python
from typing import Any

from dorm.migrations.operations import RunPython


def fill_slugs(app_label: str, registry: dict[str, Any]) -> None:
    Article = registry[f"{app_label}.Article"]
    for a in Article.objects.filter(slug=""):
        a.slug = slugify(a.title)
        a.save(update_fields=["slug"])


class Migration:
    dependencies = [("blog", "0003_add_slug")]
    operations = [RunPython(fill_slugs, reverse_code=RunPython.noop)]
```

### Contrato del callable de `RunPython`

dorm pasa **exactamente dos argumentos posicionales** a cada
callable que entregues a `RunPython(code=, reverse_code=)`. Tipa
ambos para que el editor cace los errores antes de aplicar la
migración:

```python
def my_step(app_label: str, registry: dict[str, Any]) -> None: ...
```

| Posición | Nombre | Tipo | Qué es |
|---|---|---|---|
| 1 | `app_label` | `str` | El app label al que pertenece la migración (p.ej. `"blog"`). Úsalo para construir las claves de `registry` en lugar de hardcodear el nombre de la app — así el mismo callable se reusa en forks de la misma app. |
| 2 | `registry` | `dict[str, type[dorm.Model]]` | El registro **vivo** de modelos. Resuelve clases por su nombre escueto (`registry["Post"]`) o, mejor, por la clave cualificada por app (`registry["blog.Post"]` — preferida porque es inequívoca cuando dos apps declaran la misma clase). |

Lo que **no** recibes (diferencias intencionadas vs. Django):

- **No hay argumento `connection` / `schema_editor`.** Si necesitas
  SQL en bruto dentro de un paso Python, recoge tú la conexión:

  ```python
  from dorm.db.connection import get_connection
  get_connection().execute("UPDATE blog_post SET ...", [...])
  ```

  La mayor parte del código de data migrations no debería llegar
  hasta aquí — `Model.objects.filter(...).update(...)` cubre el
  caso común y es portable.

- **No hay modelo "histórico".** dorm te entrega la clase del
  modelo *actual*, no una foto congelada de cómo era el modelo en
  este punto de la cadena de migraciones. Implicación: un callable
  que referencia una columna eliminada en una migración posterior
  romperá si reproduces la historia desde cero. Mitigación —
  mantén los pasos `RunPython` pequeños, ciñe su alcance a las
  columnas que tocan, y colócalos justo después de la migración de
  esquema que introdujo esas columnas. Si necesitas defenderte
  ante cambios de esquema futuros, escribe el paso de datos como
  `RunSQL`.

### `reverse_code=`

Pásalo siempre. `RunPython` necesita un callable de reverso para
ser considerado reversible por `dorm migrate <app> <target>`; un
paso forward sin él se ejecuta, pero la migración se negará a
hacer rollback y te quedas con la mitad de datos de una migración
deshecha a medias. Dos patrones:

- Una función real de undo, con la misma signatura
  `(app_label, registry)`, que revierte lo que hizo el forward
  (p.ej. limpia la columna que el forward backfilled).
- `RunPython.noop` — un callable incorporado (con el contrato de
  dorm) que pasas cuando el forward no tiene inverso significativo.
  El caso clásico: un backfill one-shot de datos que tolera ser
  deshecho dejando las filas tal cual.

## Targets de `dorm migrate`

```bash
dorm migrate                       # aplica todo lo pendiente
dorm migrate blog                  # solo la app blog
dorm migrate blog 0005             # forward o rollback hasta 0005
dorm migrate blog 0005_add_index   # también funciona el prefijo del nombre
dorm migrate blog zero             # rollback de todas las migraciones
```

El rollback ejecuta las operaciones al revés usando el método
`backwards()` de cada una. `RunPython` necesita un argumento
`reverse_code=` para ser reversible.

## `--dry-run`: preview antes de desplegar

```bash
dorm migrate --dry-run
```

Imprime el SQL exacto que ejecutaría cada migración pendiente, sin
tocar la base de datos y sin marcarla como aplicada. El recorder
**no** se actualiza — tu siguiente `dorm migrate` sigue viendo el
mismo conjunto pendiente. Úsalo como paso de revisión pre-deploy
sobre esquemas de producción.

## `dorm showmigrations`

```text
blog
 [X] 0001_initial
 [X] 0002_post_author
 [ ] 0003_add_slug
```

Los recuadros con cruz están aplicados; los vacíos están pendientes.
Útil para detectar migraciones fuera de orden o nunca aplicadas
después de mergear una rama de larga vida.

## Squash

Después de un año de pequeñas migraciones la cadena se hace larga.
`squashmigrations` colapsa un rango en un solo archivo:

```bash
dorm squashmigrations blog 0001 0042
```

Genera `blog/migrations/0042_squashed.py` con `replaces = [...]`
listando las originales. Cuando todos los entornos hayan aplicado la
0042, puedes borrar las originales y la squashed pasa a ser el nuevo
punto de partida.

## Detección de drift de esquema

```bash
dorm dbcheck             # comprueba todas las apps
dorm dbcheck blog users  # solo apps concretas
```

Compara el esquema de la BD viva (nombres y tipos de columna leídos
de `information_schema` / `pragma`) contra lo que esperan tus
modelos. Reporta drift como:

- columnas que el modelo declara pero la BD no tiene (migración
  olvidada)
- columnas que la BD tiene pero el modelo no (tabla editada a mano)
- tipos que no coinciden (alguien hizo `ALTER TYPE` fuera de la
  herramienta de migraciones)

Sale con código distinto de cero al detectar drift, así puedes
engancharlo a CI o a un gate pre-deploy. **No** arregla nada — su
trabajo es decírtelo.

## Concurrencia: advisory locks

`dorm migrate` toma un advisory lock de PostgreSQL
(`pg_advisory_lock`) antes de aplicar nada, así que dos workers de CI
compitiendo no aplicarán por duplicado ni corromperán el recorder.
SQLite se serializa con file locking, que tiene el mismo efecto en
setups pequeños de dev.

## Migraciones manuales: `RunPython` + `RunSQL` juntos

Cuando una misma migración mezcla SQL en bruto con un paso de datos
en Python, declara ambos dentro de `operations`. Los callables de
`RunPython` siguen el mismo contrato documentado en
[Migraciones vacías para data migrations](#migraciones-vacias-para-data-migrations)
arriba — `(app_label: str, registry: dict[str, Any]) -> None`.

```python
from typing import Any

from dorm.migrations.operations import RunPython, RunSQL


def backfill_slug_lower(app_label: str, registry: dict[str, Any]) -> None:
    """Forward step: nada que rellenar — el índice lee la columna en vivo."""
    return None


def clear_slug_overrides(app_label: str, registry: dict[str, Any]) -> None:
    """Reverse step: deshace cualquier efecto de datos del forward."""
    Post = registry[f"{app_label}.Post"]
    Post.objects.filter(slug__isnull=False).update(slug="")


class Migration:
    atomic = False  # requerido para CREATE INDEX CONCURRENTLY
    dependencies = [("blog", "0007_add_slug")]
    operations = [
        RunSQL(
            "CREATE INDEX CONCURRENTLY blog_post_slug_lower ON blog_post (LOWER(slug));",
            reverse_sql="DROP INDEX IF EXISTS blog_post_slug_lower;",
        ),
        RunPython(backfill_slug_lower, reverse_code=clear_slug_overrides),
    ]
```

`RunSQL` acepta una sola sentencia o una lista. Para cosas como
`CREATE INDEX CONCURRENTLY` — que **no puede** correr dentro de una
transacción — fija `atomic = False` a nivel de clase para que el
ejecutor se salte el wrap atómico de la migración.

## Pitfalls habituales

- **Olvidar `null=True` en un campo nuevo**: dorm se niega a añadir
  una columna `NOT NULL` sin default a una tabla no vacía. O le das
  un default, o lo divides en dos migraciones: añadir nullable,
  backfill, y luego alterar a NOT NULL.
- **Renombrar un modelo**: dorm pregunta "¿renombraste X a Y?
  [y/N]". Responder "no" crea remove + add, que **borra la tabla** —
  léelo otra vez antes de pulsar y.
- **Editar una migración ya aplicada**: no lo hagas. El recorder
  hashea el contenido; si de verdad necesitas hacerlo, borra también
  la fila de `dorm_migrations` en cada entorno.

## Migraciones zero-downtime (2.1+)

Tres operaciones que ayudan a evitar `AccessExclusiveLock` en
tablas calientes:

- **`AddIndex(..., concurrently=True)`** emite
  `CREATE INDEX CONCURRENTLY` en PostgreSQL. Debe ser la única DDL
  del fichero de migración (el executor necesita saltarse el
  atomic envolvente, ya que `CONCURRENTLY` no puede correr dentro
  de una transacción).
- **`SetLockTimeout(ms=...)`** ajusta `lock_timeout` de PG para la
  ventana de la migración: las DDL que no consigan su lock a
  tiempo fallan de forma ruidosa en vez de bloquear a los
  escritores indefinidamente.
- **`ValidateConstraint(table=, name=)`** ejecuta `ALTER TABLE ...
  VALIDATE CONSTRAINT` — la segunda mitad del patrón canónico
  `NOT VALID` + `VALIDATE` para añadir FKs / CHECKs a tablas
  grandes sin `AccessExclusiveLock`.

## Restricciones y columnas calculadas

`Meta.constraints` acepta `CheckConstraint` y
`UniqueConstraint(condition=...)` (índice único parcial — el patrón
canónico de "solo una fila activa por usuario"). El autodetector
emite `AddConstraint` / `RemoveConstraint`.

`GeneratedField` declara una columna calculada por la BD (PG ≥ 12,
SQLite ≥ 3.31).
