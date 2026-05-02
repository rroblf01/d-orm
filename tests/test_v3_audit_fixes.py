"""Regression tests for the v3.0 self-audit pass.

Pin behaviour for three latent bugs found while reviewing the new
3.0 contribs / packaging:

1. ``dorm.contrib.querylog.QueryRecord`` exposes the SQL ``vendor``
   under that name (was wrongly labelled ``alias`` even though
   ``post_query`` only carries ``sender=vendor``, not the alias).
2. ``dorm.contrib.prometheus`` no longer emits an ``alias=""``
   label that would have polluted every scrape with an empty
   string. Histogram + counter use ``vendor`` only.
3. ``LocMemCache`` prefix index does NOT bucket keys without ``:``
   under the same prefix as ``"prefix:*"`` patterns — keys without
   the namespace separator are skipped from the index so a
   ``delete_pattern("foo:*")`` doesn't accidentally evict a bare
   ``"foo"``.
4. ``UserManager.create_user`` raises a real ``RuntimeError`` (not
   an ``assert``) when the manager isn't attached to a model.
"""

from __future__ import annotations

import pytest


# ──────────────────────────────────────────────────────────────────────────────
# Bug 1 — QueryRecord uses ``vendor`` (was ``alias``)
# ──────────────────────────────────────────────────────────────────────────────


def test_query_record_carries_vendor_not_alias():
    from dorm.contrib.querylog import QueryRecord

    rec = QueryRecord(
        sql="SELECT 1",
        params=None,
        vendor="postgresql",
        elapsed_ms=1.0,
        error=None,
    )
    assert rec.vendor == "postgresql"
    # ``alias`` field is gone — the signal payload doesn't carry it.
    assert not hasattr(rec, "alias")
    d = rec.to_dict()
    assert d["vendor"] == "postgresql"
    assert "alias" not in d


def test_query_log_records_vendor_from_signal_sender():
    """Round-trip: ``post_query`` is dispatched with ``sender=vendor``,
    so the captured records' ``vendor`` field must be populated."""
    from dorm.contrib.querylog import QueryLog
    from tests.models import Author

    with QueryLog() as log:
        Author.objects.filter(name="z").count()

    assert log.records, "expected at least one captured record"
    # Vendor should be one of the engines the conftest provisions.
    vendors = {rec.vendor for rec in log.records}
    assert vendors.issubset({"sqlite", "postgresql", "libsql"})
    assert all(v != "" for v in vendors), (
        "vendor field is empty — the receiver isn't reading the signal sender"
    )


# ──────────────────────────────────────────────────────────────────────────────
# Bug 2 — Prometheus exposition has no ``alias=""`` label
# ──────────────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _reset_prom():
    from dorm.contrib.prometheus import uninstall
    uninstall()
    yield
    uninstall()


def test_prometheus_exposition_does_not_emit_empty_alias_label():
    from dorm.contrib.prometheus import install, metrics_response
    from tests.models import Author

    install()
    Author.objects.filter(name="z").count()
    out = metrics_response()
    # Pre-fix the exposition contained ``alias=""`` on every sample
    # because ``post_query`` doesn't carry an alias key. Now we use
    # ``vendor`` only.
    assert 'alias=""' not in out, (
        "Prometheus exposition still emits empty alias label — "
        "the receiver should not read kwargs.get('alias')."
    )
    # Vendor label must be present and non-empty on the counter.
    assert "vendor=" in out


# ──────────────────────────────────────────────────────────────────────────────
# Bug 3 — LocMemCache prefix index ignores keys without ':'
# ──────────────────────────────────────────────────────────────────────────────


def test_locmem_delete_pattern_preserves_keys_without_colon():
    """``delete_pattern("foo:*")`` MUST NOT evict a bare ``"foo"``
    entry — the glob requires at least one character after the
    colon. Pre-fix the prefix index bucketed both ``"foo"`` and
    ``"foo:bar"`` under the same prefix and the fast path dropped
    them together."""
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache({"OPTIONS": {"maxsize": 16}})
    c.set("foo", b"bare")
    c.set("foo:1", b"a")
    c.set("foo:2", b"b")
    n = c.delete_pattern("foo:*")
    assert n == 2
    # The bare ``"foo"`` survives.
    assert c.get("foo") == b"bare"
    assert c.get("foo:1") is None
    assert c.get("foo:2") is None


def test_locmem_keys_without_colon_not_indexed():
    """Sanity: keys without ``:`` don't land in any prefix bucket,
    so the secondary index stays focused on namespaced keys."""
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("plain-key", b"x")
    # No prefix bucket should appear — the helper returns ``None``
    # for keys without ``:`` and the index_add path skips them.
    assert c._by_prefix == {}


def test_locmem_delete_pattern_full_scan_still_evicts_unindexed():
    """A non-prefix glob falls back to ``fnmatch`` over every key,
    so unindexed entries still get reached when the pattern asks."""
    from dorm.cache.locmem import LocMemCache

    c = LocMemCache()
    c.set("plain", b"x")
    c.set("ns:1", b"y")
    n = c.delete_pattern("p*")
    assert n == 1
    assert c.get("plain") is None
    assert c.get("ns:1") == b"y"


# ──────────────────────────────────────────────────────────────────────────────
# Bug 4 — UserManager.create_user raises RuntimeError, not assert
# ──────────────────────────────────────────────────────────────────────────────


def test_user_manager_unbound_raises_runtime_error():
    """Strip ``assert`` semantics with ``python -O`` would silently
    pass an unbound manager through; the explicit isinstance check
    keeps the failure mode consistent in optimised builds."""
    from dorm.contrib.auth.models import UserManager

    mgr = UserManager()  # NOT attached to any model class
    assert mgr.model is None
    with pytest.raises(RuntimeError, match="manager"):
        mgr.create_user(email="x@x.com", password="x")


# ──────────────────────────────────────────────────────────────────────────────
# Improvement — FIELD_ENCRYPTION_KEYS is per-instance
# ──────────────────────────────────────────────────────────────────────────────


def test_field_encryption_keys_lives_on_instance():
    """Two ``Settings`` instances must NOT share the same list — a
    class-level mutable default would silently pool key rotations
    across them."""
    from dorm.conf import Settings

    a = Settings()
    b = Settings()
    a.FIELD_ENCRYPTION_KEYS.append("test-key-A")
    assert "test-key-A" not in b.FIELD_ENCRYPTION_KEYS
