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
| `--dry-run` | imprime solo el SQL; no toca la BD ni actualiza el recorder |
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
