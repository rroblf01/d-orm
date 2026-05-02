# Security policy

## Supported versions

| Version | Supported          |
|---------|--------------------|
| 3.0.x   | :white_check_mark: |
| 2.5.x   | :white_check_mark: (security only, until 2026-11-01) |
| < 2.5   | :x:                |

dorm follows [Semantic Versioning](https://semver.org/). Patch
releases on the latest minor get the full bug-fix flow; the previous
minor receives security-only patches for ~6 months after a new minor
ships.

## Reporting a vulnerability

**Please do NOT file a public GitHub issue for a security
vulnerability.** Public discussion gives an attacker a window
between disclosure and a patched release on PyPI.

Email `ricardo.r.f@hotmail.com` with:

1. A short description of the issue (one paragraph is fine).
2. The affected versions (run `python -c "import dorm; print(dorm.__version__)"`).
3. A reproduction case: minimum Python code, model definition, or
   request that triggers the bug. The smaller, the faster we
   triage.
4. The impact you observed (RCE, SQL injection, data leak,
   denial-of-service, …) and any constraints on exploitation
   (auth required, specific backend, …).

We aim to:

- Acknowledge your report within **72 hours**.
- Confirm or rule out the issue within **7 days**.
- Ship a patched release within **30 days** for a confirmed
  high-severity issue (CVSS ≥ 7.0). Lower-severity fixes ride
  the next normal patch release.
- Credit you in the release notes unless you ask us not to.

## Hardening guidance

dorm runs Python code that you control plus database drivers, so
the hot ORM path is generally not the most fruitful attack surface.
A few areas deserve explicit attention in production:

- **Cache payload signing** is on by default since 2.5.0 — the
  queryset cache HMAC-signs every blob before it leaves the
  process. Set `CACHE_SIGNING_KEY` (or the existing `SECRET_KEY`)
  in production; deploy with `CACHE_REQUIRE_SIGNING_KEY = True` so
  the per-process random fallback is refused. Without a shared
  key, a multi-worker deployment silently collapses to per-worker
  visibility — and *with* a key but writeable Redis, an attacker
  who can write to Redis gets RCE on every reader. The HMAC closes
  that window.
- **`settings.py` auto-discovery** executes Python from the
  filesystem. Pass `--settings=myproj.settings` (or
  `DORM_SETTINGS=…`) explicitly in production runners and audit
  container images for stray `settings.py` files.
- **`execute_streaming()`** refuses to run inside `atomic()` —
  server-side cursors need their own transaction. This is by
  design (the previous silent fallback materialised the whole
  result set in memory); don't paper over the error by stripping
  the `atomic()`.
- **`dorm.contrib.encrypted`** ciphertexts are bound to a single
  `FIELD_ENCRYPTION_KEY` (or the rotation list
  `FIELD_ENCRYPTION_KEYS`). Rotation does not re-encrypt existing
  rows automatically — schedule a background job to rewrite them
  through the new primary key once you've decided to retire the
  old one.
- **`dorm lint-migrations`** is meant as a CI gate. Run it on
  every PR that touches a migration; suppression
  (`# noqa: DORM-M00X`) should require a reviewer's sign-off so
  the rule doesn't get silenced by accident.

## Out of scope

- Vulnerabilities in upstream packages (psycopg, redis-py,
  cryptography, …). Report those to their maintainers; we will
  bump our pins as soon as fixed releases are out.
- Bugs that require an attacker who already has shell access on
  the server, write access to your settings file, or admin
  privileges in the database. Those need fixing at the
  infrastructure layer.
- DoS by configuring a deliberately small `MAX_POOL_SIZE` or by
  issuing pathological queries. Pool sizing and query review are
  operator concerns.
