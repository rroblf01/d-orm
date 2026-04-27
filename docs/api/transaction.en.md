# Transactions

::: dorm.transaction.atomic
::: dorm.transaction.aatomic
::: dorm.transaction.on_commit
::: dorm.transaction.aon_commit

## Forcing a rollback: `set_rollback`

The context manager returned by `atomic()` and `aatomic()` exposes
`set_rollback(True)` to force a rollback at exit time *without raising
an exception*. Useful for test fixtures and speculative-write patterns:

```python
with dorm.transaction.atomic() as tx:
    Author.objects.create(name="speculative", age=1)
    if not is_useful(...):
        tx.set_rollback(True)
    # Block exits cleanly; rollback fires anyway.
```

When `set_rollback(True)` is called, any pending `on_commit` callbacks
scheduled inside that block are discarded — same behaviour as a
real exception-driven rollback.

The async variant exposes the same method on the `aatomic()` context
manager, with identical semantics.

## `on_commit` / `aon_commit` semantics

`on_commit(callback, using="default")` schedules a zero-arg callable
to run **after** the surrounding transaction commits. Outside an
`atomic()` block, the callback fires immediately. Inside nested
blocks, callbacks are deferred all the way to the outermost commit;
a rollback at any depth discards the callbacks scheduled in that
block (and any merged from nested commits).

A failing callback is logged on the `dorm.transaction` logger but
does **not** propagate — by the time it runs the DB has already
committed and raising would falsely claim the transaction failed.

`aon_commit` accepts both regular callables and coroutine functions;
coroutine results are awaited at outermost-commit time. Outside an
`aatomic()` block, coroutine callbacks are scheduled with
`asyncio.ensure_future` so the call site doesn't have to await.
