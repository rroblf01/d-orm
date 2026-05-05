# Changelog — pytest-djanorm

Notable changes to the djanorm pytest plugin. Format:
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
versioning: [SemVer](https://semver.org/).

## [Unreleased]

## [0.1.0] - 2026-05-05

Initial release. Pairs with **djanorm 4.0**.

### Added

- Auto-discovered via the `pytest11` entry-point — installing
  the package wires the fixtures in. Disable per-project with
  `addopts = -p no:djanorm` in `pyproject.toml`.

### Fixtures

- **`djanorm_settings`** (session) — default DATABASES dict,
  in-memory SQLite. Override in `conftest.py` for real backends.
- **`pg_container`** (session) — Postgres testcontainer (or
  reads `DORM_TEST_PG_HOST` etc. when CI provides a server).
  Skips when Docker / `testcontainers[postgres]` aren't
  available.
- **`transactional_db`** (function) — wraps each test in
  `atomic()` and rolls back on teardown so writes don't leak
  between tests.
- **`atransactional_db`** (function) — async equivalent.
- **`nplusone_guard`** (function) — yields a configured
  `dorm.contrib.nplusone.NPlusOneDetector(threshold=5,
  raise_on_detect=True)` for use as
  `with nplusone_guard:` inside a test.

### Pytest hooks

- Custom markers registered: `djanorm_pg`, `djanorm_async` so
  `pytest -m` doesn't warn.

### Optional extras

- `[postgres]` — `testcontainers[postgres]>=4.14.2`
- `[mysql]` — `testcontainers[mysql]>=4.14.2`
- `[async]` — `pytest-asyncio>=1.3.0`

### Compatibility

- `pytest >= 8.0`
- `djanorm >= 4.0, < 5.0`
- Python 3.11 / 3.12 / 3.13 / 3.14
