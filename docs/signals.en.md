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

## Async caveat

Signals are **synchronous**. They fire for both sync and async
operations, but the handler is invoked with a plain function call —
you cannot `await` inside it.

```python
# Wrong — the coroutine is created and immediately dropped
async def bad(sender, **kw):
    await something_async()

post_save.connect(bad)   # nothing awaits the coroutine; warnings everywhere

# Right — schedule it on the running loop
def good(sender, instance, **kw):
    import asyncio
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return       # we're in a sync context, fall back to sync work
    loop.create_task(do_something_async(instance))
```

For most observability cases, a non-blocking enqueue is enough; the
real I/O happens in a background task you control.

## Built-in side effects

dorm itself does **not** subscribe to its own signals — they exist
purely for user code. That means:

- Disabling a signal (e.g. by `disconnect`-ing all receivers) never
  breaks ORM operations.
- A handler that raises does not block a save / delete / query — the
  exception is swallowed (see below).

## Gotchas

- **Handler exceptions are silently swallowed by `Signal.send()`.**
  This is deliberate — a buggy `post_save` listener shouldn't take
  down a request. But it means *you* are responsible for logging
  failures inside the handler. Wrap the body in `try` / `except`
  and log to your own observability stack.
- **`pre_save` cannot abort the save.** Raising inside `pre_save`
  is swallowed; the INSERT/UPDATE still runs. If you need to veto
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
