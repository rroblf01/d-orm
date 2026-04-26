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
from dorm.migrations.operations import RunPython

def fill_slugs(apps, connection):
    Article = apps.get_model("blog", "Article")
    for a in Article.objects.all():
        a.slug = slugify(a.title)
        a.save(update_fields=["slug"])

class Migration:
    dependencies = [("blog", "0003_add_slug")]
    operations = [RunPython(fill_slugs, reverse_code=RunPython.noop)]
```

`apps.get_model(app, name)` devuelve un modelo *histórico* — es
decir, el modelo con la forma de campos que tenía **en este punto de
la cadena de migraciones**. Esto es lo que protege a las data
migrations de romperse cuando luego edites el modelo en vivo.

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

## Migraciones manuales: `RunPython` / `RunSQL`

```python
from dorm.migrations.operations import RunPython, RunSQL

class Migration:
    dependencies = [("blog", "0007_add_slug")]
    operations = [
        RunSQL(
            "CREATE INDEX CONCURRENTLY blog_post_slug_lower ON blog_post (LOWER(slug));",
            reverse_sql="DROP INDEX IF EXISTS blog_post_slug_lower;",
        ),
        RunPython(my_python_function, reverse_code=my_undo_function),
    ]
```

`RunSQL` acepta una sola sentencia o una lista. Para cosas como
`CREATE INDEX CONCURRENTLY` (que no puede correr dentro de una
transacción), marca la migración con `atomic = False` a nivel de
clase.

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
