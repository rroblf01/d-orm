# Signals

Signals let you hook into model lifecycle events (`save`, `delete`)
and into every SQL query without coupling those concerns to the model
code. dorm ships six built-in signals; the API mirrors Django's.

## When to use a signal (and when not to)

**Use a signal when:**

- the hook is *cross-cutting* — auditing, cache invalidation, search
  indexing, tracing, metrics — and you don't want to add a method
  call to every place that saves;
- third-party code needs to react to your models without touching
  them.

**Don't use a signal when:**

- the logic *belongs* to the model — override `save()` / `clean()`
  instead. Signals are loosely-coupled by design and that makes
  control flow harder to follow;
- you need a guaranteed return value or to abort the operation —
  `pre_save` exceptions get swallowed (see [Gotchas](#gotchas)).

## The six built-in signals

All live in `dorm.signals`. They fire identically for **sync and
async** operations: the handlers themselves are always plain
synchronous callables.

| Signal | Fires | `sender` is | Extra kwargs |
|---|---|---|---|
| `pre_save` | before `save()` / `asave()` runs SQL | the **model class** | `instance`, `raw=False`, `using`, `update_fields` |
| `post_save` | after the INSERT/UPDATE returns | model class | `instance`, `created` (bool), `raw=False`, `using`, `update_fields` |
| `pre_delete` | before `delete()` / `adelete()` runs SQL | model class | `instance`, `using` |
| `post_delete` | after the DELETE returns | model class | `instance`, `using` |
| `pre_query` | before any SQL statement executes | the **vendor string** (`"postgresql"` / `"sqlite"`) | `sql`, `params` |
| `post_query` | after the SQL completes (or raises) | vendor string | `sql`, `params`, `elapsed_ms`, `error` |

A few notes on the kwargs:

- **`instance`** is the live model instance, not a copy — mutating it
  in `pre_save` *is* visible to the SQL that follows. That's the
  pattern for "auto-set a slug if missing".
- **`created`** in `post_save` is `True` if the row was just inserted,
  `False` for updates. It's the cleanest way to distinguish the two
  without re-querying.
- **`raw=False`** is reserved for future fixture-loading support; for
  now it's always `False`. Match Django's signature so handlers
  written for Django port over.
- **`using`** is the database alias the operation hit (`"default"`,
  `"replica"`, etc.) — useful for routing-aware handlers.
- **`error`** in `post_query` is the exception that was raised (or
  `None` if the statement succeeded). Always check it before
  treating `elapsed_ms` as a successful query timing.

## Receiver signature

Always two parts: positional `sender`, then `**kwargs`. You can
unpack the kwargs you care about and ignore the rest with `**_`.

```python
def my_handler(sender, **kwargs):
    instance = kwargs["instance"]
    created = kwargs.get("created", False)
    ...
```

The reason for the `**kwargs` catch-all: dorm may add new keyword
arguments to a signal in the future (see `update_fields`, which was
added without breaking older receivers). A handler that lists every
argument explicitly will start raising `TypeError` the day a new one
appears. **Always end the signature with `**kwargs`** (or
`**_` if you ignore everything besides `sender`).

## Connecting and disconnecting

```python
from dorm.signals import post_save

def audit(sender, instance, created, **kw):
    AuditLog.objects.create(
        model=sender.__name__,
        pk=instance.pk,
        action="created" if created else "updated",
    )

post_save.connect(audit, sender=Article)
```

`Signal.connect(receiver, sender=None, weak=True, dispatch_uid=None)`:

| Argument | Effect |
|---|---|
| `receiver` | the callable; signature `def fn(sender, **kwargs)` |
| `sender` | only invoke when `send()` was called with this sender. Typical pattern: `sender=Article` so the handler only fires for `Article` saves, not every model |
| `weak` | default `True`. dorm holds a `WeakRef` to the receiver, so a method handler whose owning object gets garbage-collected disappears automatically. Set `False` for module-level functions you want to keep alive forever (and to silence the WeakMethod warning if your handler is a bound method whose owner you can't keep alive otherwise) |
| `dispatch_uid` | a stable string identity. Connecting *again* with the same `dispatch_uid` **replaces** the previous registration. Use it for module-import-time `connect()` calls so a re-import doesn't double-register |

Disconnect via any of:

```python
post_save.disconnect(audit)                 # by receiver
post_save.disconnect(sender=Article)         # all handlers for this sender
post_save.disconnect(dispatch_uid="audit-x")  # by uid
```

## `@receiver` decorator pattern

dorm doesn't ship a `@receiver` decorator (Django's adds nothing
behavioural — it just calls `signal.connect`). You can do the same
in two lines:

```python
def receiver(signal, **kwargs):
    def deco(fn):
        signal.connect(fn, **kwargs)
        return fn
    return deco

@receiver(post_save, sender=Article, dispatch_uid="reindex-articles")
def reindex(sender, instance, **kw):
    search.index(instance)
```

## Observability with `pre_query` / `post_query`

These two fire around **every** SQL statement — sync or async — so
they're the integration point for OpenTelemetry, Datadog, structlog,
or anything that needs per-query metrics.

```python
from dorm.signals import post_query

def trace(sender, sql, params, elapsed_ms, error, **kw):
    log.info(
        "query",
        vendor=sender,            # "postgresql" / "sqlite"
        ms=elapsed_ms,
        ok=error is None,
        sql=sql,
    )

post_query.connect(trace, weak=False, dispatch_uid="apm-trace")
```

A few hard rules:

- **Keep handlers cheap.** They run inline in the query path. A slow
  handler on `post_query` slows down every database call. If you
  need to publish metrics over the network, push the work to a
  queue (`asyncio.Queue`, ThreadPoolExecutor) and return.
- **Don't issue more queries from inside a query signal.** That's an
  infinite loop. If you really need a stored sample, append to an
  in-memory ring buffer and persist out-of-band.
- **`error` is `None` on success.** Handlers that always read
  `elapsed_ms` for timing should still check `error is not None`
  before classifying the call as "slow query" — failed queries
  often look fast because they short-circuit.

## Async receivers

Receivers can be `async def` coroutine functions. Connect them the
same way as a regular handler — dorm detects coroutines via
`inspect.iscoroutinefunction` at dispatch time:

```python
import asyncio
from dorm.signals import post_save

async def index_in_search(sender, instance, created, **kw):
    await search_client.upsert(instance)

post_save.connect(index_in_search, sender=Article, weak=False)
```

The dispatch path is split in two:

- **`Model.asave()` / `Model.adelete()`** call `Signal.asend()` under
  the hood. Sync receivers are called directly; async receivers are
  awaited *sequentially*, in the order they were connected. This
  matches Django's behaviour and keeps shared-state handlers
  predictable. If you want concurrency, fan out to `asyncio.gather`
  *inside* one receiver.
- **`Model.save()` / `Model.delete()`** stay on the synchronous path.
  An async receiver registered there has no event loop to run on, so
  dorm logs a single `WARNING` on `dorm.signals` and skips it instead
  of silently dropping work or deadlocking on `asyncio.run`.

```python
# Will fire from asave / adelete:
async def audit(sender, instance, **kw):
    await audit_log.append(instance.pk, "saved")

post_save.connect(audit, sender=Order, weak=False)

await Order(...).asave()   # audit() runs
Order(...).save()          # audit() skipped + warning logged
```

You can also call `Signal.asend()` directly for custom signals:

```python
from dorm.signals import Signal

deployed = Signal()

async def notify_slack(sender, **kw):
    await slack.post(f"deployed {sender}")

deployed.connect(notify_slack, weak=False)
await deployed.asend(sender="prod-v2.1")
```

`asend()` returns the same `[(receiver, return_value), …]` shape as
`send()`. A coroutine returned by a *sync* receiver is awaited
transparently, so wrapping helpers don't drop pending work.

### Query signals stay synchronous

`pre_query` / `post_query` are dispatched from inside the SQL log
context manager, which is shared by the sync and async backends.
Async receivers connected to them are skipped with a warning — wire
async tracing through `post_save` / `post_delete` (or schedule a task
from a thin sync receiver).

## Built-in side effects

dorm itself does **not** subscribe to its own signals — they exist
purely for user code. That means:

- Disabling a signal (e.g. by `disconnect`-ing all receivers) never
  breaks ORM operations.
- A handler that raises does not block a save / delete / query — the
  exception is logged at `ERROR` on the `dorm.signals` logger, but
  the calling code continues (see below).

## Receiver failure handling

By default, an exception raised by a receiver is logged via the
`dorm.signals` logger at `ERROR` level (with full traceback) and
then suppressed so a single broken handler can't take down a save
or delete path. To wire that into your observability stack:

```python
import logging

# Send dorm signal failures to Sentry / DataDog / your handler
logging.getLogger("dorm.signals").addHandler(your_alert_handler)
```

If you'd rather have the exception propagate — useful in tests, or
for custom signals where a failed handler should fail the operation
— construct the signal with `raise_exceptions=True`:

```python
from dorm.signals import Signal

strict_event = Signal(raise_exceptions=True)
strict_event.connect(handler)
strict_event.send(sender=obj)   # any handler error is re-raised
```

The built-in signals (`pre_save`, `post_save`, `pre_delete`,
`post_delete`, `pre_query`, `post_query`) keep the legacy
log-and-suppress behaviour to preserve compatibility.

## Gotchas

- **Handler exceptions are logged, not silently swallowed.** A buggy
  `post_save` listener no longer disappears into the void; it's
  recorded on the `dorm.signals` logger so you can route it to
  Sentry / your alerting pipeline. If you want strict propagation,
  use a custom `Signal(raise_exceptions=True)` (see above).
- **`pre_save` cannot abort the save.** Raising inside `pre_save`
  is logged but the INSERT/UPDATE still runs. If you need to veto
  an operation, do it in `Model.clean()` (called by
  `full_clean()`) or before calling `save()` at all.
- **Recursion.** A `post_save` handler that calls `instance.save()`
  re-fires `pre_save` / `post_save` and can loop forever. Use
  `update_fields` to limit the new save (it skips re-firing for
  fields not in the list when you're careful), or guard with a
  thread-local flag.
- **Sender identity matters.** `pre_save` filtering uses `is`
  comparison: `connect(handler, sender=Article)` only matches
  saves of `Article`, **not** subclasses of `Article`. If you have
  abstract base mixins (`TimestampedModel`), connect to each
  concrete subclass.
- **Module re-imports double-register weak handlers.** If your
  `connect()` lives at module top level and the module gets
  reloaded (Jupyter, dev hot-reload), the handler is registered
  twice. Use `dispatch_uid` to make it idempotent.

## Reference

Full API + per-signal kwargs in the [API reference](api/signals.md).
