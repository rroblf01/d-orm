# Sibling packages: `djanorm-mypy` and `pytest-djanorm`

Two essential ecosystem integrations live in their own packages,
**not in the main wheel**:

| Package | Repo | Install |
|---------|------|---------|
| **`djanorm-mypy`** | [`djanorm-mypy/`](https://github.com/rroblf01/d-orm/tree/main/djanorm-mypy) | `pip install djanorm-mypy` |
| **`pytest-djanorm`** | [`pytest-djanorm/`](https://github.com/rroblf01/d-orm/tree/main/pytest-djanorm) | `pip install pytest-djanorm` |

This page explains **why** they were extracted — the decision is
intentional, not a packaging accident.

## Why the split matters

### 1. Zero mandatory deps in the main wheel

`djanorm` installed via `pip install djanorm` only pulls:

- the standard library;
- the backend driver you asked for (`[sqlite]` / `[postgresql]` / …).

If the plugins lived under `dorm.contrib.mypy` and
`dorm.contrib.pytest`, you'd be stuck with two bad options:

1. **Conditional imports** inside `dorm`: the code is there but
   raises at runtime if you forgot to install `mypy`/`pytest`.
   Average users pay the cognitive cost (mysterious
   `ImportError`) without getting any benefit.
2. **Hard deps** in the wheel: every `pip install djanorm`
   pulls `mypy` (~6 MB) and `pytest` (~3 MB) into the
   production container. Bigger Docker images, more transitive
   CVE surface, no runtime benefit.

Extracting them makes the rule obvious:

```bash
# Production only
pip install "djanorm[postgresql]"

# Dev: add the plugins
pip install djanorm-mypy pytest-djanorm
```

### 2. Independent versioning

`mypy` and `pytest` break their public API across minor
releases with some regularity. `djanorm-mypy 0.2` can pin
`mypy>=1.13,<2` while `djanorm-mypy 0.3` migrates to
`mypy>=2`.

If the plugin lived inside the main wheel, **every mypy bump
would force a `djanorm` release** — absurd: a change in a dev
tool should not cascade into a release of the production ORM.
Separation breaks that calendar coupling.

### 3. Different release cadences

- `djanorm` (core ORM) — slow, conservative releases. A mishaped
  query can break thousands of apps; every release runs the
  full suite against SQLite/PG/MySQL/libsql.
- `djanorm-mypy` — iterative, fast bug fixes when mypy emits
  false positives. Comfortable monthly cadence.
- `pytest-djanorm` — fixtures evolve with real-world use; a
  faster release loop than the core.

Three trains, three speeds, three `pyproject.toml` files.

### 4. Cross-version compatibility

`djanorm-mypy` declares
`dependencies = ["mypy>=1.13", "djanorm>=3.4"]`. If a future
`djanorm 4.x` changes the `Field[T]` descriptor,
`djanorm-mypy 1.0` ships with `djanorm>=4`. Users still on
`djanorm 3.x` keep `djanorm-mypy 0.x`, no disruption.

Impossible to do when both live in the same wheel.

### 5. Clean auto-discovery

`pytest-djanorm` registers its fixtures via the `pytest11`
entry-point in its own `pyproject.toml`:

```toml
[project.entry-points.pytest11]
djanorm = "pytest_djanorm.plugin"
```

Pytest discovers the plugin **only** when `pytest-djanorm` is
installed. No magic, no ad-hoc `conftest.py` in the main
wheel: the user's test suite knows the fixtures only when they
explicitly asked for the package.

Same for `mypy`:

```toml
[tool.mypy]
plugins = ["djanorm_mypy"]
```

When `djanorm-mypy` isn't installed, mypy fails with a clear
*"Plugin not installed"* error. No silent failures, no reduced
check set running quietly.

### 6. Image size and security surface

| Metric | Bundled | Extracted |
|--------|---------|-----------|
| `djanorm` wheel | +~9 MB deps | unchanged |
| Cold-start imports in prod | +`mypy`, +`pytest` | unchanged |
| Inherited CVEs | mypy + pytest | unchanged |
| `djanorm-mypy` opt-in | n/a | yes |

In Lambda containers, Cloud Run revisions, edge functions —
where cold-start time and image size matter — adding 9 MB for
a tool that **never** runs in production is unacceptable.

## When NOT to keep a sibling package

The split has a real cost: two repos to publish, two
changelogs, two CI pipelines. It is worth doing when:

- the dep is **dev-only** (mypy, pytest) — yes.
- the dep is **opt-in with niche audience** (an exotic broker)
  — yes.
- the dep is **runtime-universal** (psycopg) — no, ship in
  the wheel.

That's why the main wheel still bundles the DB-layer
``contrib`` modules (``bulk_copy``, ``listen_notify``, etc.):
they are runtime ORM code. Dev tooling (test/type-check) lives
out.

## Migrating from older versions

If older projects of yours imported something like
``from dorm.contrib.testing import …`` or
``from dorm.contrib.mypy import …`` — those modules **never**
shipped publicly, so there's no break. Just install the
siblings:

```bash
pip install pytest-djanorm djanorm-mypy
```

And follow the recipes in their respective READMEs.

## Summary

> Keep the wheel small. Extract every opt-in dev-tool to its
> own package. Version each one independently. Users only pay
> for what they use.
