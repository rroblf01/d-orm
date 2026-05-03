# Transactions

dorm exposes `transaction.atomic` (sync) and `transaction.aatomic`
(async). Both can be used as a context manager or as a decorator,
and both nest using SAVEPOINTs so an inner failure doesn't roll back
the outer block.

## Usage

```python
from dorm import transaction

# Context manager
with transaction.atomic():
    author = Author.objects.create(name="Alice", age=30)
    Book.objects.create(title="...", author=author)

# Decorator
@transaction.atomic
def transfer(src_id: int, dst_id: int, amount: int) -> None:
    Account.objects.filter(pk=src_id).update(balance=F("balance") - amount)
    Account.objects.filter(pk=dst_id).update(balance=F("balance") + amount)

# Per-alias
@transaction.atomic("replica_writer")
def write_to_replica() -> None:
    ...
```

The semantics mirror Django: any exception raised inside the block
triggers rollback; clean exit commits.

## Async

```python
from dorm.transaction import aatomic

async with aatomic():
    a = await Author.objects.acreate(name="Alice", age=30)
    await Book.objects.acreate(title="...", author=a)

@aatomic
async def transfer(...): ...

@aatomic("replica_writer")
async def replica_op(...): ...
```

Async atomic blocks acquire an async connection from the pool, so you
can hold one across `await` points without blocking the event loop.

## Nesting and savepoints

```python
with transaction.atomic():           # BEGIN
    Author.objects.create(name="A")

    try:
        with transaction.atomic():   # SAVEPOINT
            Author.objects.create(name="B")
            raise RuntimeError("rollback inner")
    except RuntimeError:
        pass                         # ROLLBACK TO SAVEPOINT — A still alive

    Author.objects.create(name="C")  # commits with A
# COMMIT
```

Author A and C end up persisted; B is rolled back to its savepoint.
This is useful for "best-effort" sub-steps inside a larger transaction.

## `atomic(durable=True)` (3.1+)

Pass `durable=True` to assert that *this* atomic block is the
outermost one — the surrounding code must NOT already be inside
another `atomic()`. Raises `RuntimeError` immediately if it would
silently degrade to a savepoint:

```python
with transaction.atomic(durable=True):  # ok — top-level
    process_payment()
    schedule_emails()

# Mistake: nested durable will raise instead of silently being
# a savepoint.
with transaction.atomic():
    with transaction.atomic(durable=True):  # RuntimeError
        ...
```

Use this on work that MUST land in its own `COMMIT` (write-then-
publish patterns where the publish step waits on a real fsync,
or where a downstream consumer reads the row by polling on a
replica). Mirrors Django's flag added in 3.2.

The async counterpart `aatomic(durable=True)` enforces the same
invariant on `async with` blocks.

## Choosing the right boundary

Keep transactions **short** and **focused on writes**:

- A web request that does N reads and 1 write only needs the write
  inside `atomic()`.
- Long transactions hold row locks → other writers block → tail
  latency spikes.
- Don't wrap entire HTTP handlers in `atomic()` "for safety". A
  network timeout or external API call inside the block holds the
  transaction open the whole time.

## Read replicas: `using=`

If your `DATABASES` has multiple aliases, `atomic("alias")` runs the
transaction on a specific connection pool. This is how you keep
writes on a primary and reads on a replica without confusing the
transaction state.

```python
@transaction.atomic("primary")
def create_post(...):
    Post.objects.using("primary").create(...)
```

`Manager.using(alias)` and `QuerySet.using(alias)` route a single
query; `transaction.atomic(alias)` routes the whole block.

For routing rules across the app, see the `DATABASE_ROUTERS` setting
in [Production deployment](production.md).

## Auto-commit and explicit transactions

dorm runs in **auto-commit by default** — every statement outside an
`atomic()` block commits immediately. You don't need to wrap simple
reads or single-statement writes; `atomic()` exists for the cases
where multiple statements must succeed-or-fail as a unit.

## Side effects after commit: `on_commit`

Sending an email, enqueueing a Celery / RQ job, publishing a Kafka
message, calling a third-party API — these effects must NEVER fire
when their parent transaction rolls back. Wrap them in
`transaction.on_commit(callback)` so they only run after a successful
commit:

```python
from dorm import transaction

with transaction.atomic():
    user = User.objects.create(name=name, email=email)
    transaction.on_commit(lambda: send_welcome_email(user))
    # If anything below raises, the user is rolled back AND
    # the email is never sent. The two are atomic together.
    audit_log.record(user, action="signup")
```

Outside an `atomic()` block, `on_commit` runs the callback
immediately (Django parity). Nested `atomic()` blocks defer all
callbacks to the outermost commit — a rollback at any depth discards
the callbacks scheduled inside it.

For async code, use `transaction.aon_commit`:

```python
from dorm import transaction

async with transaction.aatomic():
    user = await User.objects.acreate(name=name)
    transaction.aon_commit(lambda: notify_kafka(user))
    # async coroutines are awaited in order at outermost commit
```

`aon_commit` accepts both regular callables and coroutine functions —
the latter are awaited at commit time.

A failing post-commit callback is **logged on the
`dorm.transaction` logger but does not raise**: by the time it runs,
the DB has already committed and propagating the error would falsely
claim the transaction failed. Wire that logger into your alerting if
the callback is correctness-critical.

## Cleanup on rollback: `on_rollback`

The mirror image of `on_commit` — schedule a callback that fires
**only** when the surrounding transaction rolls back. Use it to undo
non-transactional side effects whose parent DB work didn't stick:
deleting a file you just wrote to local storage / S3, removing a
key from a cache, sending a "the previous notification was reverted"
webhook.

```python
from dorm import transaction

with transaction.atomic():
    user = User.objects.create(name=name)
    s3_key = upload_avatar(user, image_bytes)
    # If anything below raises, the row rolls back AND the
    # avatar gets removed — atomic together.
    transaction.on_rollback(lambda: s3.delete(s3_key))
    audit_log.record(user, action="signup")
```

Semantics mirror `on_commit` in reverse:

- **Outside an `atomic()` block**, `on_rollback` is a no-op — there's
  nothing to roll back, so nothing to undo. (Mirror of `on_commit`'s
  "fire immediately" path: same logical answer, since the
  "transaction" is already final.)
- **Inside nested `atomic()`**, callbacks fire when *their* block
  rolls back. A savepoint rollback fires only inner callbacks; an
  outer rollback fires both inner-merged and outer ones in order.
- **If the surrounding block commits**, queued rollback callbacks
  are discarded.
- **A failing rollback callback is logged**, not raised — same
  rationale as `on_commit`. The rollback already happened; losing a
  stray cleanup shouldn't escalate to a crash.

For async code, use `transaction.aon_rollback`:

```python
from dorm import transaction

async with transaction.aatomic():
    user = await User.objects.acreate(name=name)
    s3_key = await aupload_avatar(user, image_bytes)
    transaction.aon_rollback(lambda: s3_async.delete(s3_key))
```

`aon_rollback` accepts both regular callables and coroutine
functions — coroutines are awaited at rollback time.

### Built-in user: `FileField`

`FileField.pre_save` registers an `on_rollback` automatically when
it writes a file inside an `atomic()` block, so this pattern Just
Works:

```python
with transaction.atomic():
    doc = Document(name="report")
    doc.attachment = dorm.ContentFile(b"PDF body", name="r.pdf")
    doc.save()                     # writes to storage, queues cleanup
    raise BusinessRuleViolation()  # row + bytes both rolled back
```

No orphan files on disk, no orphan keys in S3 / MinIO. The
auto-registration is opt-in via being inside an active `atomic()` —
non-transactional saves are unchanged. See
[Models: Files](models.md#files) for storage backend details.

## Forcing a rollback without raising: `set_rollback`

The atomic context manager exposes `set_rollback(True)` to force a
rollback while still exiting the `with` block normally — primarily
for test fixtures and "speculative work" patterns:

```python
with transaction.atomic() as tx:
    Author.objects.create(name="speculative")
    if not is_useful(...):
        tx.set_rollback(True)
    # Block exits without an exception; rollback fires anyway,
    # the speculative row is gone, and pending on_commit callbacks
    # are discarded.
```

The `dorm.test.transactional_db` fixture is built on top of this.

## Connection-level vs alias-level

A few things to know about the model:

- `atomic()` checks out a connection, begins a transaction, runs your
  code, and commits/rolls back when the block exits.
- Nested `atomic()` calls on the same alias reuse the same connection
  and emit `SAVEPOINT` / `RELEASE SAVEPOINT` / `ROLLBACK TO SAVEPOINT`
  instead of new `BEGIN` / `COMMIT`.
- Concurrent calls on different aliases hit different pools — they
  are independent transactions and **dorm does not coordinate them**.
  If you need true cross-DB atomicity, do it in your application
  layer with sagas / outbox patterns.

## Pitfalls

- **Mixing sync and async on the same alias inside one request**: the
  sync `atomic()` and async `aatomic()` go through different pools.
  A statement issued in one is invisible to the transaction running
  on the other.
- **Catching exceptions inside the block but expecting commit**:
  swallowing an exception still commits — `atomic()` only rolls back
  on exceptions that *propagate out* of the block.
- **Long-running `atomic()` around external I/O**: holds locks open
  during the slow part. Move the I/O outside the block whenever you
  can.
- **`execute_script()` ends the surrounding transaction on SQLite**:
  SQLite's `executescript()` always issues a `COMMIT` before and
  after the script, so calling `connection.execute_script(...)` from
  inside `atomic()` / `aatomic()` ends the outer transaction — any
  earlier statements in the block are committed and can no longer be
  rolled back. This is a SQLite limitation, not a dorm bug. Use
  single-statement `connection.execute(...)` calls when you need full
  transactional control. PostgreSQL is unaffected.
