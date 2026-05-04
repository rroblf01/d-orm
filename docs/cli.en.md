# CLI reference

dorm ships a single entry point: the `dorm` command. Every subcommand
loads your settings module (auto-discovered via the
`DORM_SETTINGS_MODULE` env var, or `--settings`) and your
`INSTALLED_APPS`, then dispatches.

```text
dorm <command> [options]
```

## `dorm init`

Scaffold a new project in the current directory.

```bash
dorm init                  # creates settings.py
dorm init --app blog       # creates settings.py + blog/ with example User model
```

The generated `settings.py` is SQLite by default; switch the
`DATABASES["default"]` block to PostgreSQL when you're ready.

## `dorm makemigrations`

Detect model changes and write a migration file.

```bash
dorm makemigrations                       # all installed apps
dorm makemigrations blog users            # specific apps
dorm makemigrations --empty --name backfill_slugs blog
```

| Flag | Purpose |
|---|---|
| `--empty` | create a blank `RunPython` / `RunSQL` template |
| `--name NAME` | suffix for the file name (default: derived from operations) |
| `--merge` (3.2+) | resolve a fork in the migration graph by writing a no-op merge migration whose `dependencies = [...]` references every leaf — see below |
| `--enable-pgvector` | scaffold a migration that enables the pgvector extension |
| `--settings PATH` | settings module to load |

### Resolving merge conflicts in the migration graph (`--merge`)

Two developers branch off `0001_initial` and each land their own
`0002_*`. After the merge commit the graph forks: both `0002_a` and
`0002_b` declare `0001_initial` as their parent and neither
references the other, so the loader can't linearise apply order.

```bash
dorm makemigrations --merge                # default name: NNNN_merge.py
dorm makemigrations --merge --name reconcile_branches
```

`--merge` writes a new empty migration whose `dependencies = [...]`
lists every leaf, collapsing the fork back to a linear graph. The
merge migration carries no operations — it only re-points the
graph's tip; the diverging migrations stay applied as-is.

No-op when the graph is already linear (zero or one leaf): prints
`No migration conflicts detected — every app has at most one leaf.
Nothing to merge.` so it's safe to wire into CI / pre-merge gates.

## `dorm migrate`

Apply pending migrations or roll back to a target.

```bash
dorm migrate                       # apply everything pending
dorm migrate blog                  # only the blog app
dorm migrate blog 0005             # forward or rollback to 0005
dorm migrate blog 0005_add_index   # name prefix also works
dorm migrate blog zero             # rollback every migration
```

| Flag | Purpose |
|---|---|
| `--dry-run` / `--plan` (3.0+) | print SQL only; don't touch the DB or update the recorder. ``--plan`` is an alias kept for users coming from Django |
| `--fake` (3.0+) | record every pending migration as applied without running its operations. Use when adopting dorm against a hand-managed legacy schema |
| `--fake-initial` (3.0+) | only fake the *initial* migration of each app, and only when its ``CreateModel`` target tables already exist |
| `--run-syncdb` (3.1+) | create tables for INSTALLED_APPS that ship NO migration files (legacy / hand-managed apps). Useful when adopting dorm incrementally |
| `--prune` (3.1+) | drop recorder rows for migrations whose source files no longer exist on disk (e.g. after `squashmigrations`). No DDL — only the bookkeeping is touched |
| `--verbosity N` | 0 = silent, 1 = default, 2 = verbose |
| `--settings PATH` | settings module to load |

## `dorm showmigrations`

List every migration and its applied status.

```text
blog
 [X] 0001_initial
 [X] 0002_post_author
 [ ] 0003_add_slug
```

```bash
dorm showmigrations                # all apps
dorm showmigrations blog           # one app
```

## `dorm squashmigrations`

Collapse a contiguous range of migrations into one.

```bash
dorm squashmigrations blog 0042
dorm squashmigrations blog 0010 0042
dorm squashmigrations blog 0010 0042 --squashed-name initial
```

The result is `<app>/migrations/<end>_<name>.py` with
`replaces = [...]` listing the originals. Once every environment has
applied the squashed migration, you can delete the originals.

## `dorm sql`

Print the `CREATE TABLE` DDL for a model.

```bash
dorm sql users.User                # one model
dorm sql users.User blog.Post      # several
dorm sql --all                     # every model in INSTALLED_APPS
```

Useful for sharing schemas with DBAs, seeding fixtures, or generating
the SQL needed to bootstrap a non-managed read replica.

## `dorm dbcheck`

Compare model definitions against the live database.

```bash
dorm dbcheck                       # all apps
dorm dbcheck blog users            # specific apps
```

Reports drift (missing columns, hand-edited types, columns the model
doesn't know about) and exits non-zero on any difference. Wire this
into CI or a pre-deploy gate to catch missing migrations early.

## `dorm shell`

Open an interactive Python REPL with dorm pre-configured.

```bash
dorm shell
```

If IPython is installed, you get IPython; otherwise the standard
REPL. Settings are loaded and `INSTALLED_APPS` are imported, so you
can `from blog.models import Post` and start querying right away.

## `dorm lint-migrations` (3.0+)

Walk every migration in `INSTALLED_APPS` and emit findings for known
online-deploy footguns. Exits non-zero on findings — wire as a CI
pre-merge gate.

```bash
dorm lint-migrations
dorm lint-migrations --format json            # JSON for CI tools
dorm lint-migrations --rule DORM-M001         # only this rule
dorm lint-migrations --rule DORM-M001 --rule DORM-M003
dorm lint-migrations --exit-zero              # advisory: never fail CI
```

| Flag | Purpose |
|---|---|
| `--format text\|json` | output shape (default: text) |
| `--rule CODE` | restrict to a code; may repeat |
| `--exit-zero` | exit 0 even when findings exist |
| `--settings PATH` | settings module to load |

Suppress a finding for a single file with a `# noqa: DORM-M00X`
comment anywhere in the file. See the
[Migration safety](production.en.md#migration-safety-dorm-lint-migrations)
section in the production guide for the full rule table.

## `dorm dbshell`

Drop into the underlying database client (`psql` or `sqlite3`) with
credentials and database name pre-filled from settings.

```bash
dorm dbshell                      # connects to DATABASES["default"]
dorm dbshell --database replica   # pick a different alias
```

The PostgreSQL password is passed via `PGPASSWORD` env var rather
than the connection string so it doesn't end up in your shell
history or `ps` output. The child process inherits your terminal —
exit it (`\q` for psql, `.exit` for sqlite3) to come back.

## `dorm dumpdata` (2.1+)

Serialise model rows to JSON. With no positional argument every
concrete model in `INSTALLED_APPS` is dumped. Pass an app label or
`app.ModelName` to scope.

```bash
dorm dumpdata                              # everything → stdout
dorm dumpdata blog                         # only models in app "blog"
dorm dumpdata blog.Post users.User         # specific models
dorm dumpdata --output fixtures/seed.json --indent 2
```

Output format (compatible with Django's `dumpdata`):

```json
[
  {"model": "blog.Author", "pk": 1, "fields": {"name": "Alice"}},
  {"model": "blog.Article", "pk": 7, "fields": {
      "title": "Hello", "author": 1, "tags": [3, 5]
  }}
]
```

Foreign keys serialise as the target's primary-key value. M2M
relations serialise as a list of related PKs. Non-JSON-native types
(decimals, UUIDs, datetimes, durations, ranges, bytes) round-trip
through dedicated envelopes — the loader rebuilds the right Python
type via the field's `to_python`.

## `dorm loaddata` (2.1+)

Load one or more JSON fixture files into the database.

```bash
dorm loaddata fixtures/seed.json
dorm loaddata fixtures/users.json fixtures/posts.json
dorm loaddata fixtures/seed.json --database replica
```

Each file is loaded inside a single transaction — a malformed record
rolls back to that file's start instead of leaving a partial restore.
M2M relations are inserted in a second phase, after every parent row
has landed. **`save()` and signals are bypassed** for performance;
`Model.save()` is the right path when you do want pre-save hooks to
fire.

## `dorm help`

```bash
dorm help          # full subcommand list
dorm <cmd> --help  # per-command flags
```

## Settings discovery

Every command resolves settings in this order:

1. `--settings dotted.path.to.settings`
2. `DORM_SETTINGS_MODULE=dotted.path.to.settings` env var
3. A `settings.py` next to the working directory (last-resort)

If none of these resolve, dorm exits with an explanatory error.

## `dorm inspectdb` (2.1+)

Reverse-engineer a `models.py` snippet from the connected database.
Best-effort recovery of field types, FK references and `db_table`;
constraints / indexes / `related_name` / validators are *not*
introspected. Pipe the output into a file and edit::

    dorm inspectdb > legacy/models.py

Use `--database alias` to introspect a non-default `DATABASES` entry.

## `dorm doctor` (2.1+)

Audit the running configuration for production-mode footguns:
small `MAX_POOL_SIZE`, missing `sslmode` on remote PostgreSQL hosts,
foreign keys without an index, transient-error retry left disabled.
Exits non-zero on any warning, so it doubles as a pre-deploy gate::

    dorm doctor

The doctor is conservative — it only warns when the rule of thumb
is widely accepted. Tune to your workload before treating any single
warning as gospel.

## `dorm createsuperuser` (3.1+)

Mint a `dorm.contrib.auth.User` row with `is_superuser=True`. Pass
`--password` for non-interactive flows; omit it for an interactive
prompt that confirms the input.

```bash
dorm createsuperuser --email admin@example.com --password 'secret'
dorm createsuperuser --email admin@example.com   # interactive prompt
```

## `dorm changepassword` (3.1+)

Update an existing user's password:

```bash
dorm changepassword admin@example.com --password 'newsecret'
dorm changepassword admin@example.com   # interactive prompt
```

Hash comparison is constant-time via `hmac.compare_digest`. The new
hash uses the configured default algorithm (PBKDF2 stdlib, or
Argon2 if `[auth-argon2]` is installed).

## `dorm flush` (3.1+)

Delete every row from every managed table. Schema stays — only the
data goes. Confirms unless `--noinput` is passed:

```bash
dorm flush --noinput
```

PostgreSQL uses `TRUNCATE … RESTART IDENTITY CASCADE`; SQLite +
MySQL fall back to `DELETE FROM`.

## `dorm sqlmigrate` (3.1+)

Print the SQL of a single migration without applying it:

```bash
dorm sqlmigrate myapp 0007_add_index
dorm sqlmigrate myapp 0007_add_index --backwards
```

Useful for review before running a sensitive migration on
production data. The recorder is **not** updated.

## `dorm shell_plus` (3.1+)

Alias for `dorm shell` — Django-extensions parity. The base `shell`
already auto-imports every model from `INSTALLED_APPS` into the
namespace, so the two commands behave identically; `shell_plus` is
exposed as muscle-memory friendly entry point.

## `dorm runscript` (3.1+)

Run a Python file under the project's settings, with INSTALLED_APPS
preloaded. Mirrors django-extensions' `runscript`:

```bash
dorm runscript path/to/ops.py [args...] [--settings myproj.settings]
```

Extra positional args are forwarded as `sys.argv[1:]` so scripts
read CLI args the way they would under a normal interpreter.
