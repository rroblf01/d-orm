# djanorm-mypy

mypy plugin for [djanorm](https://github.com/rroblf01/d-orm).

## Install

```bash
pip install djanorm-mypy
```

## Enable

`pyproject.toml`:

```toml
[tool.mypy]
plugins = ["djanorm_mypy"]
```

Or `mypy.ini`:

```ini
[mypy]
plugins = djanorm_mypy
```

## What it does

- **Validate `filter()` / `exclude()` / `get()` kwargs** against the model's
  field set. ``Author.objects.filter(naem="x")`` reports an error.
- **Lookup suffix validation** — `name__icontains` is fine,
  `name__contians` is flagged.
- **`pk` / `id` attribute** synthesised on every Model subclass so
  `obj.pk` types correctly even when the primary key is an
  `AutoField` / `UUIDField` / `CompositePrimaryKey`.
- **Field descriptor narrowing** — `Author.name` is `CharField`,
  `author.name` is `str`. The runtime overloads already do this, but
  the plugin keeps it consistent across third-party Field subclasses.

## License

MIT.
