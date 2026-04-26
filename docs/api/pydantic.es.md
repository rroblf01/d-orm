# Interop con Pydantic

Adaptador para generar esquemas Pydantic v2 a partir de modelos dorm.
Pensado para FastAPI / Starlette.

> Las firmas son código Python; la prosa es traducción manual.

## `schema_for`

```python
def schema_for(
    model_cls: Type[Model],
    *,
    name: str | None = None,
    exclude: tuple[str, ...] = (),
    only: tuple[str, ...] | None = None,
    optional: tuple[str, ...] = (),
    base: Type[BaseModel] = BaseModel,
) -> Type[BaseModel]
```

Genera un `BaseModel` de Pydantic v2 que refleja *model_cls*. El
resultado tiene `model_config = ConfigDict(from_attributes=True)`, así
que Pydantic puede leer valores directamente desde una instancia
dorm — puedes pasar un objeto dorm a `Schema.model_validate(obj)` o
usarlo como `response_model` de FastAPI.

| Argumento | Efecto |
|---|---|
| `name` | nombre de la clase generada (default `f"{Model.__name__}Schema"`) |
| `exclude` | tupla de campos a omitir |
| `only` | si se da, incluye **solo** estos campos (mutuamente excluyente con `exclude`) |
| `optional` | marca esos campos como `Optional[...] = None` aunque la columna sea NOT NULL — útil para bodies de PATCH |
| `base` | `BaseModel` base (p.ej. para compartir `ConfigDict` entre esquemas) |

`ManyToManyField` siempre se excluye (vive en una tabla intermedia).
Si necesitas serializar M2M, declara una lista anotada en un wrapper.

```python
from dorm.contrib.pydantic import schema_for

AuthorOut = schema_for(Author)                                # todos los campos
AuthorIn = schema_for(Author, exclude=("id", "created_at"))   # body de POST
AuthorPatch = schema_for(Author, optional=("name", "age"))    # body de PATCH
```

## `DormSchema`

```python
class DormSchema(BaseModel, metaclass=DormSchemaMeta): ...
```

Versión declarativa: subclasea `DormSchema` y declara `class Meta`
con el modelo objetivo. La metaclase recorre `_meta.fields` y añade
anotaciones para cada columna que no hayas declarado tú mismo.

### `class Meta` opciones

| Opción | Efecto |
|---|---|
| `model` | **obligatorio** — la clase modelo de origen |
| `fields` | tupla con whitelist de campos, o `"__all__"` (default) |
| `exclude` | tupla de campos a omitir (excluyente con `fields`) |
| `optional` | marca esos campos como `Optional[...] = None` |
| `nested` | dict `{nombre_fk: SubSchema}` — usa el sub-esquema en vez del PK entero. Para M2M usa `nested={"tags": TagOut}` y se serializa como `list[TagOut]` |

```python
from dorm.contrib.pydantic import DormSchema

class PublisherOut(DormSchema):
    class Meta:
        model = Publisher
        fields = ("id", "name")

class AuthorOut(DormSchema):
    bio_url: str | None = None     # campo extra declarado a mano

    class Meta:
        model = Author
        exclude = ("internal_notes",)
        nested = {"publisher": PublisherOut}
```

`from_attributes=True` se fija automáticamente — pasa una instancia
dorm a `AuthorOut.model_validate(obj)` directamente.

## Mapeo de tipos

| Campo dorm | Tipo Python en el esquema |
|---|---|
| `IntegerField` y subclases | `int` |
| `FloatField` | `float` |
| `DecimalField` | `Decimal` |
| `BooleanField` | `bool` |
| `CharField`, `TextField`, `EmailField`, `URLField`, `SlugField`, IP fields | `str` |
| `UUIDField` | `UUID` |
| `DateField` | `date` |
| `TimeField` | `time` |
| `DateTimeField` | `datetime` |
| `JSONField` | `Any` |
| `BinaryField` | `bytes` |
| `ForeignKey`, `OneToOneField` | `int` (la PK), o el sub-esquema si está en `Meta.nested` |
| `ManyToManyField` | excluido por defecto; con `Meta.nested={"tags": T}` → `list[T]` |
| `AutoField`, `BigAutoField`, etc. | `int` (siempre opcional) |

Un campo se marca como **Optional con default None** si:

- la columna es `null=True`, **o**
- es un `AutoField` / PK con auto-incremento, **o**
- aparece en `Meta.optional`, **o**
- el campo tiene `default=` o `default=callable`

Es decir: cualquier cosa que el caller pueda omitir del payload de
entrada sin romper.

---

> Para la versión auto-generada desde docstrings (en inglés), mira
> [Pydantic interop (English)](../../api/pydantic/).
