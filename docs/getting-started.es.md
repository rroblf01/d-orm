# Empezando

Recorrido de 10 minutos desde "no lo he instalado" hasta "he insertado
y consultado filas de verdad". Sin FastAPI ni async — solo lo básico.
Para la versión async / FastAPI salta al [Tutorial](tutorial.md).

## 1. Instalar

```bash
pip install "djanorm[sqlite]"
# o con uv (recomendado):
uv add "djanorm[sqlite]"
```

Para PostgreSQL: `pip install "djanorm[postgresql]"`.
Para MySQL / MariaDB (3.1+): `pip install "djanorm[mysql]"`
(pure-Python `pymysql` + `aiomysql`, sin C toolchain).
Para uploads en S3: `pip install "djanorm[s3]"` (funciona con AWS S3,
MinIO, Cloudflare R2, Backblaze B2).

## 2. Crear el esqueleto del proyecto

```bash
mkdir miapp && cd miapp
dorm init --app blog
```

Esto crea:

```
.
├── blog/
│   ├── __init__.py
│   └── models.py        # User de ejemplo
└── settings.py          # bloques DATABASES y STORAGES comentados
```

El `settings.py` generado incluye plantillas comentadas tanto para
SQLite/PostgreSQL como para `STORAGES` de almacenamiento de archivos
(disco local, AWS S3 y MinIO/S3-compatible). Descomenta las que
necesites.

## 3. Configurar la base de datos

Abre `settings.py` y descomenta la sección de SQLite:

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "sqlite",
        "NAME": "blog.db",
    }
}
```

dorm autodescubre cualquier directorio hermano que tenga `__init__.py`
+ `models.py`, así que para el caso simple no necesitas `INSTALLED_APPS`.

## 4. Definir tus modelos

Edita `blog/models.py`:

```python title="blog/models.py"
import dorm


class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    email = dorm.EmailField(unique=True)
    bio = dorm.TextField(null=True, blank=True)

    class Meta:
        ordering = ["name"]


class Post(dorm.Model):
    title = dorm.CharField(max_length=200)
    body = dorm.TextField()
    author = dorm.ForeignKey(Author, on_delete=dorm.CASCADE, related_name="posts")
    published = dorm.BooleanField(default=False)
    created_at = dorm.DateTimeField(auto_now_add=True)
```

## 5. Crear y aplicar migraciones

```bash
dorm makemigrations
dorm migrate
```

Verás:

```
Detecting changes for 'blog'...
  Created migration: blog/migrations/0001_initial.py
  Applying blog.0001_initial... OK
```

## 6. Insertar y consultar

Entra en la shell de dorm — pre-importa tus modelos y usa IPython
si está disponible:

```bash
dorm shell
```

```python
>>> alice = Author.objects.create(name="Alice", email="alice@example.com")
>>> Post.objects.create(title="Hello", body="World", author=alice, published=True)
<Post: pk=1>

>>> Author.objects.count()
1

>>> for p in Post.objects.filter(published=True).select_related("author"):
...     print(p.author.name, "—", p.title)
Alice — Hello

>>> # Expresiones F, objetos Q, agregaciones — todo está aquí
>>> from dorm import F, Q, Count
>>> Author.objects.annotate(post_count=Count("posts")).values_list("name", "post_count")
[('Alice', 1)]
```

## 7. Pasar a PostgreSQL

Cuando estés listo para dejar SQLite, lo único que cambia es
`settings.py`:

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "postgresql",
        "NAME": "blog",
        "USER": "postgres",
        "PASSWORD": "secreto",
        "HOST": "localhost",
        "PORT": 5432,
    }
}
```

Lanza `dorm migrate` contra la BD vacía de PG. Tu código, modelos y
consultas no cambian.

## 8. MySQL / MariaDB (3.1+)

Instala el extra y apunta al servicio MySQL:

```bash
pip install "djanorm[mysql]"
```

```python title="settings.py"
DATABASES = {
    "default": {
        "ENGINE": "mysql",   # o "mariadb"
        "NAME": "blog",
        "USER": "root",
        "PASSWORD": "secreto",
        "HOST": "localhost",
        "PORT": 3306,
    }
}
```

Caveats: DDL no es transaccional en MySQL — envolver `ALTER TABLE`
en `atomic()` no lo revierte. `RETURNING` funciona en MariaDB
10.5+ pero no en MySQL; el insert usa `cursor.lastrowid` para PKs
autoincrement. El wrapper fuerza `ANSI_QUOTES` para que los
identificadores entre comillas dobles parseen igual que en
PostgreSQL / SQLite.

## ¿Qué sigue?

- [Modelos y campos](models.md) — todos los tipos y sus opciones
- [Consultas](queries.md) — filter, exclude, Q, F, agregaciones
- [Patrones async](async.md) — `acreate`, `aiterator`, `aatomic`
- [Tutorial](tutorial.md) — montarlo con FastAPI
- [Subida de archivos con `FileField`](models.md#archivos) — disco
  local por defecto, cambia a S3 / MinIO / R2 con un cambio en
  `STORAGES`
