# `dorm.contrib.asyncguard`

Surfaces sync ORM calls executed inside a running event loop —
the bug pattern that turns a fast async route into a slow one
because every other request on the worker stalls while the sync
query blocks.

The guard hooks `pre_query` and walks the call stack: a sync ORM
call inside a coroutine triggers the configured action
(``"warn"`` / ``"raise"`` / ``"raise_first"``); legitimate `a*`
async calls stay silent.

## When to enable

- **Tests / dev**: ``"warn"`` (or ``"raise"`` if you want hard
  failures) — catches the bug at design time.
- **Production**: leave disabled. Stack walking on every query has
  a small cost; you'd rather pay a bit at dev to never ship the
  pattern in the first place.

```python
# conftest.py — opt in for the whole test suite
from dorm.contrib.asyncguard import enable_async_guard

def pytest_configure(config):
    enable_async_guard(mode="warn")
```

## Modes

| Mode             | Behaviour                                                               |
|------------------|-------------------------------------------------------------------------|
| `"warn"`         | Single `WARNING` per call site (template-deduped).                      |
| `"raise"`        | Raises :class:`SyncCallInAsyncContext` on every offender.               |
| `"raise_first"`  | Raises once, then degrades to `"warn"` so logs aren't drowned.          |

`SyncCallInAsyncContext` inherits from :class:`BaseException` so it
bypasses `Signal.send`'s `except Exception` and surfaces as a 500
instead of a logged-and-swallowed warning.

## API

::: dorm.contrib.asyncguard.enable_async_guard
::: dorm.contrib.asyncguard.disable_async_guard
::: dorm.contrib.asyncguard.SyncCallInAsyncContext
