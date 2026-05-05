# Changelog — djanorm-mypy

Notable changes to the djanorm mypy plugin. The package follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and
[Semantic Versioning](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-05-05

Initial release. Pairs with **djanorm 4.0**.

### Added

- `DjanormPlugin` registered via `djanorm_mypy:plugin` —
  configure with:
  ```toml
  [tool.mypy]
  plugins = ["djanorm_mypy"]
  ```
- **Filter kwarg validation** — `Manager.filter()`,
  `Manager.exclude()`, `Manager.get()`,
  `QuerySet.filter()`/`exclude()`/`get()`, plus the async
  variants (`afilter`, `aexclude`, `aget`,
  `get_or_create`, `aget_or_create`, `update_or_create`,
  `aupdate_or_create`). Each kwarg is checked against the
  resolved model's field set.
- **Lookup-suffix validation** — `name__icontains` is fine,
  `name__contians` fails. Whitelist covers the standard set:
  `exact` / `iexact` / `contains` / `icontains` / `startswith` /
  `istartswith` / `endswith` / `iendswith` / `regex` / `iregex` /
  `gt` / `gte` / `lt` / `lte` / `in` / `isnull` / `range` /
  `year` / `month` / `day` / `hour` / `minute` / `second` /
  `week` / `weekday` / `iso_week_day` / `quarter` / `date` /
  `time` / `len` / `has_key` / `has_keys` / `has_any_keys` /
  `contained_by` / `overlap`.
- **`pk` / `id` synthesis** on every `dorm.Model` subclass —
  consumer code can rely on `obj.pk` typing as `Any` regardless
  of the concrete primary-key column type (`AutoField`,
  `UUIDField`, `CompositePrimaryKey`, …).
- **`Field[T]` descriptor narrowing** — class-level access
  returns `Field[_T]`, instance-level access narrows to `_T`.
  The runtime overloads in djanorm already do this; the plugin
  reinforces the behaviour for third-party `Field` subclasses
  that don't carry the overloads themselves.

### Compatibility

- `mypy >= 1.13`
- `djanorm >= 4.0, < 5.0`
- Python 3.11 / 3.12 / 3.13 / 3.14
