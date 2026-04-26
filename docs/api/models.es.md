# Modelos y campos

Referencia de la clase base `dorm.Model` y todos los tipos de campo.

> **Nota:** los nombres de clase y firmas son código Python (idénticos
> en cualquier idioma). Las descripciones aquí son traducción manual.

## `dorm.Model`

Clase base para todos los modelos. La metaclase construye el registro
`_meta`, instala los descriptors de campo y el manager `objects`.

### Métodos de instancia

```python
def save(self, *, force_insert: bool = False, update_fields: list[str] | None = None) -> None
async def asave(self, *, force_insert: bool = False, update_fields: list[str] | None = None) -> None
```

Persiste la instancia en BD. Hace `INSERT` si la PK es `None`, si no
`UPDATE`. Pasa `update_fields=["col1", "col2"]` para limitar el
`UPDATE` a esas columnas (más rápido y safe contra escrituras
concurrentes en otros campos).

```python
def delete(self) -> tuple[int, dict[str, int]]
async def adelete(self) -> tuple[int, dict[str, int]]
```

Borra la instancia y respeta las cadenas `on_delete=CASCADE`. Devuelve
`(num_total_borrado, {modelo: cuenta_por_modelo})`.

```python
def refresh_from_db(self, fields: list[str] | None = None) -> None
async def arefresh_from_db(self, fields: list[str] | None = None) -> None
```

Re-lee los valores desde la BD (descarta cambios locales no
guardados). Útil tras un `update()` masivo o un `auto_now`.

```python
def full_clean(self) -> None
```

Ejecuta `clean_fields()` → `clean()` → `validate_unique()`. Lanza
`ValidationError` con todos los problemas agregados.

## Tipos de campo

Cada campo es `Field[T]` (un `Generic` parametrizado por el tipo Python
que almacena). El descriptor sobrecargado hace que `Author.name` sea
`Field[str]` (introspección) y `author.name` sea `str` (valor real).

### Strings

```python
CharField(max_length: int = 255, **opts)         # VARCHAR(N)
TextField(**opts)                                 # TEXT
EmailField(**opts)                                # VARCHAR(254), valida al asignar
URLField(**opts)                                  # VARCHAR(200)
SlugField(**opts)                                 # VARCHAR(50), indexado, valida formato
UUIDField(**opts)                                 # UUID (PG) / VARCHAR(36) (SQLite)
IPAddressField(**opts)                            # solo IPv4
GenericIPAddressField(**opts)                     # IPv4 + IPv6
```

### Números

```python
IntegerField(**opts)                              # INTEGER
SmallIntegerField(**opts)                         # SMALLINT (PG) / INTEGER (SQLite)
BigIntegerField(**opts)                           # BIGINT (PG) / INTEGER (SQLite)
PositiveIntegerField(**opts)                      # INTEGER, valida value >= 0
PositiveSmallIntegerField(**opts)                 # SMALLINT, valida value >= 0
FloatField(**opts)                                # REAL
DecimalField(max_digits=10, decimal_places=2, **opts)  # NUMERIC(N, M)
```

### Tiempo

```python
DateField(**opts)                                 # DATE
TimeField(**opts)                                 # TIME
DateTimeField(auto_now=False, auto_now_add=False, **opts)  # TIMESTAMP / DATETIME
```

`auto_now_add=True` rellena al insertar (no se actualiza después).
`auto_now=True` reescribe en cada save. Ambos hacen `editable=False`
y aplican `default=lambda: datetime.now(timezone.utc)`.

### Booleanos

```python
BooleanField(**opts)                              # BOOLEAN (PG) / INTEGER (SQLite)
NullBooleanField(**opts)                          # equivalente a BooleanField(null=True)
```

### Datos estructurados

```python
JSONField(**opts)                                 # JSONB (PG) / TEXT (SQLite)
BinaryField(**opts)                               # BYTEA / BLOB
ArrayField(base_field, **opts)                    # <inner>[] (solo PG)
```

`ArrayField` lanza `NotImplementedError` en SQLite a la hora de
generar DDL — para que el límite aparezca en `migrate`, no en la
primera query de prod.

### Auto-incremento

```python
AutoField(**opts)                                 # INTEGER PRIMARY KEY AUTOINCREMENT
BigAutoField(**opts)                              # BIGSERIAL / INTEGER PK AUTOINC
SmallAutoField(**opts)                            # SMALLSERIAL / INTEGER PK AUTOINC
```

Si no declaras una PK, dorm añade automáticamente un `BigAutoField`
llamado `id`.

### Relaciones

```python
ForeignKey(to, on_delete, related_name=None, null=False, **opts)
OneToOneField(to, on_delete, related_name=None, null=False, **opts)
ManyToManyField(to, through=None, related_name=None, **opts)
```

`on_delete` acepta `CASCADE`, `PROTECT`, `SET_NULL`, `SET_DEFAULT`,
`DO_NOTHING`, `RESTRICT` — semántica idéntica a Django.

El descriptor de FK expone:

- `book.author` → la instancia `Author` relacionada (lazy fetch + caché)
- `book.author_id` → la PK entera cruda (`int | None`)

## Opciones comunes (todos los campos)

| Opción | Efecto |
|---|---|
| `null=True` | la columna permite `NULL` (a nivel BD) |
| `blank=True` | string vacío permitido (validación, no BD) |
| `unique=True` | añade restricción `UNIQUE` |
| `db_index=True` | crea un índice |
| `db_column="x"` | override del nombre de la columna |
| `default=value` o `default=callable` | valor por defecto |
| `validators=[fn, ...]` | se ejecutan al asignar y en `full_clean()` |
| `choices=[(value, label), …]` | restringe a un conjunto fijo |
| `editable=False` | oculto a forms / serializers |
| `help_text="..."` | string de docs |
| `primary_key=True` | marca como PK (sustituye al `id` implícito) |

---

> Para la versión auto-generada desde docstrings (en inglés), mira
> [Models & fields (English)](../../api/models/).
