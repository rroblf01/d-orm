# Referencia de la CLI

dorm trae un único entry point: el comando `dorm`. Cada subcomando
carga tu módulo de settings (autodescubierto vía la variable de
entorno `DORM_SETTINGS_MODULE`, o `--settings`) y tus
`INSTALLED_APPS`, y luego despacha.

```text
dorm <comando> [opciones]
```

## `dorm init`

Genera el scaffold de un proyecto nuevo en el directorio actual.

```bash
dorm init                  # crea settings.py
dorm init --app blog       # crea settings.py + blog/ con un User de ejemplo
```

El `settings.py` generado usa SQLite por defecto; cambia el bloque
`DATABASES["default"]` a PostgreSQL cuando estés listo.

## `dorm makemigrations`

Detecta cambios en los modelos y escribe un archivo de migración.

```bash
dorm makemigrations                       # todas las apps instaladas
dorm makemigrations blog users            # apps concretas
dorm makemigrations --empty --name backfill_slugs blog
```

| Flag | Para qué |
|---|---|
| `--empty` | crea una plantilla en blanco con `RunPython` / `RunSQL` |
| `--name NAME` | sufijo para el nombre del archivo (default: derivado de las operaciones) |
| `--settings PATH` | módulo de settings a cargar |

## `dorm migrate`

Aplica las migraciones pendientes o hace rollback hasta un target.

```bash
dorm migrate                       # aplica todo lo pendiente
dorm migrate blog                  # solo la app blog
dorm migrate blog 0005             # forward o rollback hasta 0005
dorm migrate blog 0005_add_index   # también vale el prefijo del nombre
dorm migrate blog zero             # rollback de todas las migraciones
```

| Flag | Para qué |
|---|---|
| `--dry-run` / `--plan` (3.0+) | imprime solo el SQL; no toca la BD ni actualiza el recorder. ``--plan`` es alias para usuarios que vienen de Django |
| `--fake` (3.0+) | registra cada migración pendiente como aplicada SIN ejecutar sus operaciones. Útil al adoptar dorm contra un schema legacy administrado a mano |
| `--fake-initial` (3.0+) | solo "fakea" la migración *inicial* de cada app, y solo si sus ``CreateModel`` apuntan a tablas que ya existen |
| `--run-syncdb` (3.1+) | crea tablas para INSTALLED_APPS sin migrations dir (apps legacy / hand-managed). Útil al adoptar dorm gradualmente |
| `--prune` (3.1+) | borra recorder rows huérfanos (migración cuyo archivo ya no existe en disco, p.ej. tras `squashmigrations`). Solo bookkeeping, sin DDL |
| `--verbosity N` | 0 = silencioso, 1 = default, 2 = verbose |
| `--settings PATH` | módulo de settings a cargar |

## `dorm showmigrations`

Lista todas las migraciones y su estado de aplicación.

```text
blog
 [X] 0001_initial
 [X] 0002_post_author
 [ ] 0003_add_slug
```

```bash
dorm showmigrations                # todas las apps
dorm showmigrations blog           # una app
```

## `dorm squashmigrations`

Colapsa un rango contiguo de migraciones en una sola.

```bash
dorm squashmigrations blog 0042
dorm squashmigrations blog 0010 0042
dorm squashmigrations blog 0010 0042 --squashed-name initial
```

El resultado es `<app>/migrations/<end>_<name>.py` con
`replaces = [...]` listando las originales. Cuando todos los
entornos hayan aplicado la squashed, puedes borrar las originales.

## `dorm sql`

Imprime el DDL `CREATE TABLE` de un modelo.

```bash
dorm sql users.User                # un modelo
dorm sql users.User blog.Post      # varios
dorm sql --all                     # cada modelo en INSTALLED_APPS
```

Útil para compartir esquemas con DBAs, seedear fixtures, o generar
el SQL necesario para levantar una réplica de solo-lectura no
gestionada por dorm.

## `dorm dbcheck`

Compara las definiciones de los modelos contra la BD viva.

```bash
dorm dbcheck                       # todas las apps
dorm dbcheck blog users            # apps concretas
```

Reporta drift (columnas que faltan, tipos editados a mano, columnas
que el modelo no conoce) y sale con código distinto de cero ante
cualquier diferencia. Engánchalo a CI o a un gate pre-deploy para
cazar migraciones olvidadas pronto.

## `dorm shell`

Abre un REPL Python interactivo con dorm preconfigurado.

```bash
dorm shell
```

Si tienes IPython instalado, lo usa; si no, cae al REPL estándar.
Los settings se cargan y las `INSTALLED_APPS` se importan, así que
puedes hacer `from blog.models import Post` y empezar a consultar.

## `dorm lint-migrations` (3.0+)

Recorre cada migración en `INSTALLED_APPS` y emite hallazgos para
patrones peligrosos en deploy online. Sale con código != 0 ante
hallazgos — engánchalo como gate de pre-merge en CI.

```bash
dorm lint-migrations
dorm lint-migrations --format json            # JSON para herramientas CI
dorm lint-migrations --rule DORM-M001         # solo esta regla
dorm lint-migrations --rule DORM-M001 --rule DORM-M003
dorm lint-migrations --exit-zero              # advisory: nunca falla CI
```

| Flag | Para qué |
|---|---|
| `--format text\|json` | shape de salida (default: text) |
| `--rule CODE` | restringe a un código; puede repetirse |
| `--exit-zero` | sale 0 aunque haya hallazgos |
| `--settings PATH` | módulo de settings a cargar |

Silencia un hallazgo en un archivo concreto con un comentario
`# noqa: DORM-M00X` en cualquier sitio del fichero. Tabla completa
de reglas en
[Seguridad de migraciones](production.es.md#seguridad-de-migraciones-dorm-lint-migrations).

## `dorm dbshell`

Cae directamente en el cliente nativo de la base de datos
(`psql` o `sqlite3`) con credenciales y nombre de BD ya cableados
desde settings.

```bash
dorm dbshell                      # conecta a DATABASES["default"]
dorm dbshell --database replica   # elige otro alias
```

La contraseña de PostgreSQL se pasa por la variable `PGPASSWORD` en
lugar de la cadena de conexión, así no queda en el historial del
shell ni en `ps`. El proceso hijo hereda tu terminal — sal con `\q`
(psql) o `.exit` (sqlite3) para volver.

## `dorm dumpdata` (2.1+)

Serializa filas de modelos a JSON. Sin argumento posicional vuelca
todos los modelos concretos de `INSTALLED_APPS`. Pasa un label de app
o `app.ModelName` para acotar.

```bash
dorm dumpdata                              # todo → stdout
dorm dumpdata blog                         # solo modelos de la app "blog"
dorm dumpdata blog.Post users.User         # modelos específicos
dorm dumpdata --output fixtures/seed.json --indent 2
```

Formato de salida (compatible con `dumpdata` de Django):

```json
[
  {"model": "blog.Author", "pk": 1, "fields": {"name": "Alice"}},
  {"model": "blog.Article", "pk": 7, "fields": {
      "title": "Hello", "author": 1, "tags": [3, 5]
  }}
]
```

Las claves foráneas se serializan como el PK del objetivo. Las
relaciones M2M se serializan como lista de PKs relacionados. Los
tipos no nativos de JSON (decimales, UUIDs, datetimes, duraciones,
rangos, bytes) viajan por envoltorios dedicados — el cargador
reconstruye el tipo Python correcto vía el `to_python` del campo.

## `dorm loaddata` (2.1+)

Carga uno o más archivos JSON de fixtures dentro de la base de datos.

```bash
dorm loaddata fixtures/seed.json
dorm loaddata fixtures/users.json fixtures/posts.json
dorm loaddata fixtures/seed.json --database replica
```

Cada archivo se carga dentro de una única transacción — un registro
malformado revierte al inicio de ese archivo en lugar de dejar una
restauración a medias. Las relaciones M2M se insertan en una segunda
fase, una vez que todas las filas padre han aterrizado. **Se omiten
`save()` y las señales** por rendimiento; `Model.save()` es el camino
correcto cuando sí quieres que disparen los pre-save hooks.

## `dorm help`

```bash
dorm help          # lista completa de subcomandos
dorm <cmd> --help  # flags por comando
```

## Descubrimiento de settings

Cada comando resuelve los settings en este orden:

1. `--settings ruta.dotted.a.los.settings`
2. Variable de entorno `DORM_SETTINGS_MODULE=ruta.dotted.a.los.settings`
3. Un `settings.py` junto al directorio de trabajo (último recurso)

Si nada de esto resuelve, dorm sale con un error explicativo.

## `dorm inspectdb` (2.1+)

Reverse-engineering de un snippet `models.py` desde la base de
datos conectada. Recupera tipos de campo, referencias FK y
`db_table` con esfuerzo razonable; constraints, índices,
`related_name` y validators **no** se introspectan. Redirige la
salida a un fichero y revísalo::

    dorm inspectdb > legacy/models.py

`--database alias` permite introspectar una entrada no-default de
`DATABASES`.

## `dorm doctor` (2.1+)

Auditoría de la configuración runtime para detectar footguns de
producción: `MAX_POOL_SIZE` pequeño, host de PostgreSQL remoto sin
`sslmode`, foreign keys sin índice, retry de errores transitorios
desactivado. Sale con código distinto de cero ante cualquier
warning, así que sirve como puerta pre-despliegue::

    dorm doctor

El doctor es conservador — solo avisa cuando la regla de oro es
ampliamente aceptada. Ajusta a tu carga antes de tratar un único
warning como dogma.

## `dorm createsuperuser` (3.1+)

Crea una fila `dorm.contrib.auth.User` con `is_superuser=True`. Pasa
`--password` para flujo no-interactivo; sin él, prompt interactivo
con confirmación.

```bash
dorm createsuperuser --email admin@example.com --password 'secret'
dorm createsuperuser --email admin@example.com   # prompt interactivo
```

## `dorm changepassword` (3.1+)

Cambia la contraseña de un usuario existente:

```bash
dorm changepassword admin@example.com --password 'newsecret'
dorm changepassword admin@example.com   # prompt interactivo
```

Comparación constant-time vía `hmac.compare_digest`. El nuevo hash
usa el algoritmo configurado por defecto (PBKDF2 stdlib, o Argon2
si `[auth-argon2]` está instalado).

## `dorm flush` (3.1+)

Borra todas las filas de toda tabla managed. Esquema queda — solo
los datos se van. Confirma salvo que pases `--noinput`:

```bash
dorm flush --noinput
```

PostgreSQL usa `TRUNCATE … RESTART IDENTITY CASCADE`; SQLite y
MySQL caen a `DELETE FROM`.

## `dorm sqlmigrate` (3.1+)

Imprime el SQL de una sola migración sin aplicarlo:

```bash
dorm sqlmigrate myapp 0007_add_index
dorm sqlmigrate myapp 0007_add_index --backwards
```

Útil para review antes de correr una migración sensible en
producción. El recorder NO se actualiza.

## `dorm shell_plus` (3.1+)

Alias de `dorm shell` — paridad con django-extensions. El `shell`
base ya auto-importa todos los modelos de `INSTALLED_APPS` al
namespace, así que ambos comandos se comportan idéntico;
`shell_plus` se expone como entrada muscle-memory-friendly.

## `dorm runscript` (3.1+)

Ejecuta un archivo Python bajo los settings del proyecto, con
INSTALLED_APPS precargado. Espejo de django-extensions
`runscript`:

```bash
dorm runscript path/to/ops.py [args...] [--settings myproj.settings]
```

Args posicionales extra se pasan como `sys.argv[1:]` para que el
script lea CLI args como bajo el intérprete normal.
