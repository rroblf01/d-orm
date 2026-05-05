# pytest-djanorm

pytest plugin for [djanorm](https://github.com/rroblf01/d-orm).

## Install

```bash
pip install pytest-djanorm                    # SQLite-only
pip install 'pytest-djanorm[postgres]'        # + testcontainers PG fixture
pip install 'pytest-djanorm[mysql]'           # + testcontainers MySQL
pip install 'pytest-djanorm[async]'           # + pytest-asyncio
```

## Fixtures

- `djanorm_settings` — session fixture; defaults to in-memory SQLite.
  Override in `conftest.py` for real backends.
- `pg_container` — session fixture; spins up a PostgresContainer (or
  reads `DORM_TEST_PG_*` env vars when CI provides a server).
- `transactional_db` / `atransactional_db` — function fixtures that
  wrap each test in an `atomic()` and roll back on teardown.
- `nplusone_guard` — yields a configured `NPlusOneDetector` that
  raises on detection.

## Quick start

```python
# tests/test_authors.py
def test_create(transactional_db):
    Author.objects.create(name="x", age=1)
    assert Author.objects.count() == 1

# Rolled back; next test sees 0 rows.
```

## License

MIT.
