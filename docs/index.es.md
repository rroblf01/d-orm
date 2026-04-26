# djanorm

Un ORM al estilo Django para Python con **async de primera clase**,
schemas de Pydantic listos para FastAPI y un CLI `dorm` ligero.
Sin dependencia del runtime de Django.

```python
import dorm

class Author(dorm.Model):
    name = dorm.CharField(max_length=100)
    age = dorm.IntegerField()

# Síncrono
alice = Author.objects.create(name="Alice", age=30)
adultos = Author.objects.filter(age__gte=18).order_by("name")

# Asíncrono — cada método tiene su variante `a*`
alice = await Author.objects.acreate(name="Alice", age=30)
async for a in Author.objects.filter(age__gte=18):
    print(a.name)
```

## Por dónde empezar

| Si eres… | Lee… |
|---|---|
| nuevo del todo | [Empezando](getting-started.md) |
| montas una app con FastAPI | [Tutorial: tu primera API en 5 min](tutorial.md) |
| vienes de Django | [Migración desde Django ORM](migration-from-django.md) |
| buscas un método | Referencia API (barra lateral) |
| vas a desplegar a producción | [Despliegue en producción](production.md) |

## Por qué dorm

- **La misma API de QuerySet que Django** — `filter`, `exclude`, `Q`,
  `F`, `bulk_create`, `select_related`, `prefetch_related`, señales,
  todo. Si conoces Django, ya conoces dorm.
- **Sync **y** async** — cada método tiene una variante `a*`. El pool
  async reintenta errores transitorios y registra consultas lentas
  sin configurar nada.
- **Con tipos** — `Field[T]` genérico + `Manager[Self]`. Tu IDE sabe
  que `user.name` es `str`, no `Any`, y detecta `user.naem` como typo.
- **Listo para FastAPI** — `DormSchema` con `class Meta: model = User`
  genera un schema Pydantic v2 que refleja tu modelo, incluyendo
  serialización anidada de FK / M2M. Sin pegamento.
- **Hardening de producción incluido** — helper de health-check,
  advisory locks en migraciones, reintento transitorio, hooks de
  observabilidad de queries (OpenTelemetry / Datadog / Prometheus),
  logs de queries lentas.
- **PostgreSQL y SQLite** — el mismo código de modelos, las mismas
  migraciones; cambias entre ellos editando una línea.

## Instalación

```bash
pip install "djanorm[sqlite]"
pip install "djanorm[postgresql]"
pip install "djanorm[sqlite,postgresql,pydantic]"
```

## Referencia rápida

- Definición de modelos → [Modelos y campos](models.md)
- API de consultas → [Consultas](queries.md)
- Patrones async → [Patrones async](async.md)
- Migraciones de schema → [Migraciones](migrations.md)
- Transacciones → [Transacciones](transactions.md)
- FastAPI / Pydantic → [Integración con FastAPI](fastapi.md)
- CLI `dorm` → [Referencia del CLI](cli.md)
- Pasar a producción → [Despliegue en producción](production.md)
- Atascado con algo → [Resolución de problemas](troubleshooting.md)
