# Upgrading from 4.2 to 4.3

No code changes required. Every v4.3 addition is opt-in.

## Compatibility

- Python 3.11+ (unchanged).
- ``djanorm-mypy`` / ``pytest-djanorm`` sibling packages keep their
  current floor pins.
- Existing migrations re-apply unchanged.

## Bug-fix behaviour changes

These were release-blocker fixes between the 4.3 cut and tag — code
that relied on the buggy behaviour might shift:

- **``Manager.upsert(returning=True)``** previously raised at the
  ``bulk_create`` boundary because it passed Field objects instead
  of names. The call now succeeds. Code that depended on the
  failure path (e.g. catching it as "not yet implemented") must
  adapt.
- **``Saga.run()`` inside ``atomic()``** now logs a WARNING. Audit
  any place that intentionally nests sagas — the documented
  semantics haven't changed, only the diagnostic.
- **``two_phase_commit``** now raises ``RuntimeError`` when called
  inside an ``atomic()`` block. Restructure to leave the atomic
  before entering 2PC (it always was unsafe — silently degraded
  behaviour is now an explicit error).

## Recommended adoption order

1. **`dorm doctor`** — picks up the new v4.3 warnings (no new
   checks added in 4.3, but the existing ones are still
   load-bearing).
2. **`dorm.contrib.permissions`** — replace ad-hoc permission
   checks with the DSL.
3. **`dorm.contrib.concurrency.named_lock`** — replace hand-rolled
   advisory-lock helpers with the shared primitive.
4. **`UUIDField(version=7)`** on new tables — time-ordered PKs
   significantly improve B-tree insert locality vs random v4 UUIDs.
5. **`SerializableSnapshot`** wherever you previously used manual
   retry loops around SERIALIZABLE blocks.
6. **TaskQueue cron + priorities** — migrate cron scripts to
   ``@task(cron="...")`` so they share the outbox+retry
   infrastructure.
7. **`dorm.contrib.slow_tx`** — flip on in staging first; the
   detector instruments every ``atomic()``, so monitor the WARNING
   log for a baseline before turning on in production.
8. **Migration linter v2** — run on every PR; new codes
   (``DORM-M005`` / ``DORM-M006``) catch real foot-guns.

## New optional dependencies

None. ``stream_msgpack`` requires ``pip install msgpack`` opt-in,
but no new dorm extra is mandatory.

## Removed / deprecated

Nothing removed. Nothing deprecated.

---

See [What's new in 4.3](v4_3.md) for the full feature catalogue.
