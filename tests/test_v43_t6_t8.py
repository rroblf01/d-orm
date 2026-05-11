"""Tier-6/7/8 features for v4.3."""
from __future__ import annotations

import pytest


# ── T6 dorm init --template ─────────────────────────────────────────────────


class TestInitTemplate:
    def test_template_choices_exposed(self):
        from dorm.cli import _TEMPLATES

        assert "fastapi-postgres" in _TEMPLATES
        assert "litestar-sqlite" in _TEMPLATES

    def test_unknown_template_exits(self, tmp_path, monkeypatch):
        import argparse

        from dorm.cli import cmd_init

        monkeypatch.chdir(tmp_path)
        with pytest.raises(SystemExit):
            cmd_init(argparse.Namespace(template="not-a-template", app=None))

    def test_template_writes_files(self, tmp_path, monkeypatch, capsys):
        import argparse

        from dorm.cli import cmd_init

        monkeypatch.chdir(tmp_path)
        cmd_init(argparse.Namespace(template="litestar-sqlite", app=None))
        assert (tmp_path / "settings.py").exists()
        assert (tmp_path / "app" / "models.py").exists()
        assert (tmp_path / "app" / "main.py").exists()
        out = capsys.readouterr().out
        assert "Created" in out


# ── T7 F.coalesce ───────────────────────────────────────────────────────────


class TestFCoalesce:
    def test_wraps_in_coalesce(self):
        from dorm.expressions import F
        from dorm.functions import Coalesce

        expr = F("nick").coalesce("anon")
        assert isinstance(expr, Coalesce)
        assert len(expr.expressions) == 2

    def test_passes_through_expressions(self):
        from dorm.expressions import F, Value
        from dorm.functions import Coalesce

        expr = F("a").coalesce(F("b"), Value("c"))
        assert isinstance(expr, Coalesce)


# ── T7 subtree_filter ──────────────────────────────────────────────────────


class TestSubtreeFilter:
    def test_subtree_filter_exported(self):
        from dorm.tree import subtree_filter

        assert callable(subtree_filter)


# ── T8 anonymizer ──────────────────────────────────────────────────────────


class TestAnonymizer:
    def test_redact_string(self):
        from dorm.contrib.anonymizer import redact

        assert redact("hi") == "[REDACTED]"
        assert redact(None) is None
        assert redact(42) is None

    def test_random_email_deterministic(self):
        from dorm.contrib.anonymizer import random_email

        a = random_email("user@example.com")
        b = random_email("user@example.com")
        assert a == b
        assert a.endswith("@example.test")

    def test_random_phone_starts_plus15(self):
        from dorm.contrib.anonymizer import random_phone

        v = random_phone("+34123456789")
        assert v.startswith("+1555")

    def test_unknown_strategy_rejected(self):
        from dorm.contrib.anonymizer import _resolve

        with pytest.raises(ValueError, match="unknown strategy"):
            _resolve("nope")

    def test_callable_strategy_accepted(self):
        from dorm.contrib.anonymizer import _resolve

        fn = _resolve(lambda v: f"X-{v}")
        assert fn("a") == "X-a"

    def test_empty_rules_rejected(self):
        import dorm
        from dorm.contrib.anonymizer import anonymize_model

        class _M(dorm.Model):
            x = dorm.CharField(max_length=10)

            class Meta:
                app_label = "tests"

        with pytest.raises(ValueError, match="rules is required"):
            anonymize_model(_M, {})


# ── T8 token rotation ──────────────────────────────────────────────────────


class TestTokenRotation:
    def test_rotate_returns_pair(self):
        from dorm.contrib.auth.tokens import rotate_short_lived_token

        new_tok, old_tok = rotate_short_lived_token("old-token")
        assert new_tok != "old-token"
        assert old_tok == "old-token"
        assert new_tok.startswith("tok_")

    def test_rotate_no_previous(self):
        from dorm.contrib.auth.tokens import rotate_short_lived_token

        new_tok, old_tok = rotate_short_lived_token(None)
        assert old_tok is None
        assert new_tok.startswith("tok_")

    def test_custom_prefix(self):
        from dorm.contrib.auth.tokens import rotate_short_lived_token

        new_tok, _ = rotate_short_lived_token(None, prefix="api_")
        assert new_tok.startswith("api_")


# ── T8 sensitive pattern extension ─────────────────────────────────────────


class TestSensitivePatternExtension:
    def test_add_and_reset(self):
        from dorm.db.utils import (
            _DEFAULT_SENSITIVE_PATTERNS,
            _SENSITIVE_COLUMN_PATTERNS,
            add_sensitive_pattern,
            reset_sensitive_patterns,
        )

        reset_sensitive_patterns()
        assert "ssn" not in _SENSITIVE_COLUMN_PATTERNS
        add_sensitive_pattern("ssn", "credit_card")
        # Re-import the module-level binding (it's reassigned on add).
        from dorm.db.utils import _SENSITIVE_COLUMN_PATTERNS as latest

        assert "ssn" in latest
        assert "credit_card" in latest
        # Idempotent.
        add_sensitive_pattern("ssn")
        from dorm.db.utils import _SENSITIVE_COLUMN_PATTERNS as latest2

        assert latest2.count("ssn") == 1
        reset_sensitive_patterns()
        from dorm.db.utils import _SENSITIVE_COLUMN_PATTERNS as restored

        assert restored == _DEFAULT_SENSITIVE_PATTERNS

    def test_empty_call_no_op(self):
        from dorm.db.utils import add_sensitive_pattern

        # Doesn't raise.
        add_sensitive_pattern()
