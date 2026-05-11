"""Tier-3 (security/operational) features added in v4.2.

Covers:
- EncryptedField key rotation
- PII auto-mask in streaming
- Audit-log append-only constraint (DDL emit shape)
- Pool saturation metric (rendering)
- Read-replica auto-failover (circuit breaker)
"""
from __future__ import annotations

import pytest


# ── MakeTableAppendOnly DDL ──────────────────────────────────────────────────


class _FakeConn:
    def __init__(self, vendor: str = "postgresql") -> None:
        self.vendor = vendor
        self.scripts: list[str] = []

    def execute_script(self, sql: str) -> None:
        self.scripts.append(sql)


class TestMakeTableAppendOnly:
    def test_pg_emits_function_and_trigger(self):
        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log")
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        joined = "\n".join(conn.scripts)
        assert "CREATE OR REPLACE FUNCTION" in joined
        assert "BEFORE UPDATE OR DELETE" in joined
        assert "append-only" in joined.lower()

    def test_pg_allow_delete_only_blocks_update(self):
        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log", allow_delete=True)
        conn = _FakeConn()
        op.database_forwards("app", conn, None, None)
        trigger = [s for s in conn.scripts if "BEFORE" in s][0]
        assert "BEFORE UPDATE ON" in trigger
        assert "DELETE" not in trigger.split("BEFORE")[1].split("ON")[0]

    def test_sqlite_emits_two_triggers(self):
        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log")
        conn = _FakeConn(vendor="sqlite")
        op.database_forwards("app", conn, None, None)
        creates = [s for s in conn.scripts if s.startswith("CREATE TRIGGER")]
        assert len(creates) == 2  # UPDATE + DELETE

    def test_reverse_drops_objects(self):
        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log")
        conn = _FakeConn()
        op.database_backwards("app", conn, None, None)
        joined = "\n".join(conn.scripts)
        assert "DROP TRIGGER IF EXISTS" in joined
        assert "DROP FUNCTION IF EXISTS" in joined

    def test_unknown_vendor_no_op(self, caplog):
        import logging

        from dorm.migrations.operations import MakeTableAppendOnly

        op = MakeTableAppendOnly("audit_log")
        conn = _FakeConn(vendor="mysql")
        with caplog.at_level(logging.WARNING, logger="dorm.migrations"):
            op.database_forwards("app", conn, None, None)
        assert conn.scripts == []
        assert any(
            "MakeTableAppendOnly" in rec.message for rec in caplog.records
        )


# ── PII auto-mask streaming ──────────────────────────────────────────────────


class TestStreamingMaskPII:
    def _build_qs_double(self):
        """Return a (qs_like, model_cls) pair where iterating qs_like
        yields dict-shaped rows. Avoids the cost of spinning a DB up
        for a pure-logic check."""
        import dorm

        class _PIIRow(dorm.Model):
            email = dorm.EmailField(pii=True)
            name = dorm.CharField(max_length=32)

            class Meta:
                app_label = "tests"

        class _QS:
            model = _PIIRow

            def __init__(self, rows):
                self._rows = rows

            def iterator(self, chunk_size: int = 1000):
                yield from self._rows

        return _QS, _PIIRow

    def test_stream_jsonl_masks_pii_field(self):
        import json

        from dorm.contrib.streaming import stream_jsonl

        QS, _PIIRow = self._build_qs_double()
        qs = QS([
            {"email": "a@b.com", "name": "alice"},
            {"email": "c@d.com", "name": "bob"},
        ])
        out = b"".join(stream_jsonl(qs, mask_pii=True)).decode("utf-8")
        records = [json.loads(line) for line in out.strip().split("\n")]
        assert records[0]["email"] == "[REDACTED]"
        assert records[1]["email"] == "[REDACTED]"
        # Non-PII columns untouched.
        assert records[0]["name"] == "alice"

    def test_stream_jsonl_no_mask_by_default(self):
        import json

        from dorm.contrib.streaming import stream_jsonl

        QS, _PIIRow = self._build_qs_double()
        qs = QS([{"email": "a@b.com", "name": "alice"}])
        out = b"".join(stream_jsonl(qs)).decode("utf-8")
        rec = json.loads(out.strip())
        assert rec["email"] == "a@b.com"

    def test_stream_json_masks_pii_field(self):
        import json

        from dorm.contrib.streaming import stream_json

        QS, _PIIRow = self._build_qs_double()
        qs = QS([{"email": "a@b.com", "name": "alice"}])
        out = b"".join(stream_json(qs, mask_pii=True)).decode("utf-8")
        payload = json.loads(out)
        assert payload[0]["email"] == "[REDACTED]"

    def test_mask_silently_skips_when_no_model(self):
        import json

        from dorm.contrib.streaming import stream_jsonl

        plain_iter = [{"email": "a@b.com"}]
        out = b"".join(stream_jsonl(plain_iter, mask_pii=True)).decode("utf-8")
        rec = json.loads(out.strip())
        # Plain iterable carries no model → mask is a no-op.
        assert rec["email"] == "a@b.com"


# ── Pool saturation ──────────────────────────────────────────────────────────


class TestPoolSaturation:
    def test_saturation_line_emitted(self, monkeypatch):
        import dorm.db.connection as conn_mod
        from dorm.contrib import prometheus

        monkeypatch.setattr(
            conn_mod,
            "pool_stats",
            lambda alias: {
                "pool_size": 5,
                "max_size": 10,
                "requests_num": None,
                "pool_available": 2,
            },
        )
        monkeypatch.setitem(conn_mod._sync_connections, "test_alias_sat", object())
        text = prometheus.metrics_response()
        assert "dorm_pool_saturation" in text

    def test_warn_when_over_threshold(self, monkeypatch, caplog):
        import logging

        import dorm.db.connection as conn_mod
        from dorm.contrib import prometheus

        monkeypatch.setattr(
            conn_mod,
            "pool_stats",
            lambda alias: {
                "pool_size": 9,
                "max_size": 10,
                "requests_num": 9,
                "pool_available": 0,
            },
        )
        monkeypatch.setitem(conn_mod._sync_connections, "test_alias_warn", object())
        with caplog.at_level(
            logging.WARNING, logger="dorm.contrib.prometheus.pool"
        ):
            prometheus.metrics_response()
        assert any("saturated" in rec.message for rec in caplog.records)


# ── LagAwareReadRouter circuit breaker ───────────────────────────────────────


class TestLagRouterCircuitBreaker:
    def test_breaker_opens_after_failures(self, monkeypatch):
        import time

        from dorm.contrib.lag_router import LagAwareReadRouter

        router = LagAwareReadRouter(
            primary="primary",
            replicas=["r1"],
            failure_threshold=2,
            cooldown_seconds=60.0,
            cache_seconds=0.001,
        )
        # Force the probe to always fail by returning ``None`` from
        # ``_measure_lag``.
        monkeypatch.setattr(router, "_measure_lag", lambda alias: None)

        assert router._is_healthy("r1") is False  # failure 1
        time.sleep(0.005)  # let cache expire so second call re-probes
        assert router._is_healthy("r1") is False  # failure 2 → opens
        snap = router.snapshot()
        assert snap["r1"]["breaker_open"] is True
        # Subsequent call must NOT re-probe (breaker open).
        calls: list[str] = []

        def _track(alias):
            calls.append(alias)
            return None

        monkeypatch.setattr(router, "_measure_lag", _track)
        router._is_healthy("r1")
        assert calls == []  # no probe issued

    def test_breaker_closes_after_success(self, monkeypatch):
        from dorm.contrib.lag_router import LagAwareReadRouter

        router = LagAwareReadRouter(
            primary="primary",
            replicas=["r1"],
            failure_threshold=2,
            cooldown_seconds=0,  # immediate retry
            cache_seconds=0.001,
        )
        monkeypatch.setattr(router, "_measure_lag", lambda alias: 0.5)
        assert router._is_healthy("r1") is True
        snap = router.snapshot()
        assert snap["r1"]["consecutive_failures"] == 0
        assert snap["r1"]["breaker_open"] is False

    def test_cooldown_zero_disables_breaker(self, monkeypatch):
        from dorm.contrib.lag_router import LagAwareReadRouter

        router = LagAwareReadRouter(
            primary="primary",
            replicas=["r1"],
            failure_threshold=2,
            cooldown_seconds=0,
            cache_seconds=0.001,
        )
        monkeypatch.setattr(router, "_measure_lag", lambda alias: None)
        for _ in range(5):
            router._is_healthy("r1")
        # Breaker never opens when cooldown is 0.
        assert router.snapshot()["r1"]["breaker_open"] is False

    def test_invalid_failure_threshold_rejected(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        with pytest.raises(ValueError, match="failure_threshold"):
            LagAwareReadRouter(
                replicas=["r1"], failure_threshold=0
            )

    def test_invalid_cooldown_rejected(self):
        from dorm.contrib.lag_router import LagAwareReadRouter

        with pytest.raises(ValueError, match="cooldown_seconds"):
            LagAwareReadRouter(
                replicas=["r1"], cooldown_seconds=-1.0
            )


# ── EncryptedField key rotation ──────────────────────────────────────────────


class TestEncryptedKeyRotation:
    def test_rotate_helper_exists(self):
        from dorm.contrib.encrypted import (
            arotate_encryption_keys,
            rotate_encryption_keys,
        )

        assert callable(rotate_encryption_keys)
        assert callable(arotate_encryption_keys)

    def test_rotate_rejects_non_encrypted_field(self):
        import dorm
        from dorm.contrib.encrypted import rotate_encryption_keys

        class _M(dorm.Model):
            name = dorm.CharField(max_length=32)

            class Meta:
                app_label = "tests"

        with pytest.raises(TypeError, match="not an EncryptedField"):
            rotate_encryption_keys(_M, fields=["name"])

    def test_rotate_no_encrypted_fields_returns_zero(self):
        import dorm
        from dorm.contrib.encrypted import rotate_encryption_keys

        class _M2(dorm.Model):
            name = dorm.CharField(max_length=32)

            class Meta:
                app_label = "tests"

        # No encrypted fields → no-op.
        assert rotate_encryption_keys(_M2) == 0
