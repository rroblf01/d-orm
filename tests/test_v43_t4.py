"""Tier-4 distributed-transaction primitives for v4.3."""
from __future__ import annotations

import pytest


# ── Saga ────────────────────────────────────────────────────────────────────


class TestSaga:
    def test_happy_path(self):
        from dorm.contrib.saga import Saga, Step

        events: list[str] = []

        def s1(ctx):
            events.append("s1")

        def s2(ctx):
            events.append("s2")

        saga = Saga(steps=[Step("s1", s1), Step("s2", s2)])
        run = saga.run()
        assert run.ok is True
        assert run.completed == ["s1", "s2"]
        assert run.compensated == []
        assert events == ["s1", "s2"]

    def test_compensation_on_failure(self):
        from dorm.contrib.saga import Saga, Step

        events: list[str] = []

        def s1(ctx):
            events.append("s1")

        def c1(ctx):
            events.append("c1")

        def s2(ctx):
            events.append("s2")

        def c2(ctx):
            events.append("c2")

        def s3(ctx):
            raise RuntimeError("boom")

        saga = Saga(
            steps=[Step("s1", s1, c1), Step("s2", s2, c2), Step("s3", s3)]
        )
        run = saga.run()
        assert run.ok is False
        assert run.failure is not None
        assert run.failure[0] == "s3"
        # Compensations fire in reverse.
        assert run.compensated == ["s2", "s1"]
        assert events[:2] == ["s1", "s2"]
        # c2 fired before c1.
        assert events.index("c2") < events.index("c1")

    def test_step_without_compensate_logs_warning(self, caplog):
        import logging

        from dorm.contrib.saga import Saga, Step

        def s1(ctx):
            pass

        def s2(ctx):
            raise RuntimeError("nope")

        saga = Saga(steps=[Step("s1", s1), Step("s2", s2)])  # no compensate
        with caplog.at_level(logging.ERROR, logger="dorm.contrib.saga"):
            run = saga.run()
        assert run.ok is False
        assert any(
            "manual intervention" in rec.message for rec in caplog.records
        )

    def test_duplicate_step_names_rejected(self):
        from dorm.contrib.saga import Saga, Step

        with pytest.raises(ValueError, match="unique"):
            Saga(steps=[Step("x", lambda c: None), Step("x", lambda c: None)])

    def test_empty_steps_rejected(self):
        from dorm.contrib.saga import Saga

        with pytest.raises(ValueError, match="at least one"):
            Saga(steps=[])

    def test_compensation_failure_continues(self, caplog):
        import logging

        from dorm.contrib.saga import Saga, Step

        events: list[str] = []

        def s1(ctx):
            events.append("s1")

        def c1(ctx):
            events.append("c1")

        def c2(ctx):
            raise RuntimeError("c2 broken")

        def s2(ctx):
            events.append("s2")

        def s3(ctx):
            raise RuntimeError("forward fail")

        saga = Saga(
            steps=[Step("s1", s1, c1), Step("s2", s2, c2), Step("s3", s3)]
        )
        with caplog.at_level(logging.ERROR, logger="dorm.contrib.saga"):
            saga.run()
        # c1 still ran even after c2 failed.
        assert "c1" in events
        assert any(
            "compensation for step 's2' failed" in rec.message
            for rec in caplog.records
        )

    def test_context_shared_across_steps(self):
        from dorm.contrib.saga import Saga, Step

        def s1(ctx):
            ctx["x"] = 1

        def s2(ctx):
            ctx["x"] += 1

        saga = Saga(steps=[Step("s1", s1), Step("s2", s2)])
        run = saga.run({})
        assert run.context["x"] == 2


# ── Two-phase commit ────────────────────────────────────────────────────────


class TestTwoPhaseCommit:
    def test_rejects_empty_aliases(self):
        from dorm.contrib.two_phase import two_phase_commit

        with pytest.raises(ValueError):
            with two_phase_commit([]):
                pass

    def test_rejects_non_pg_alias(self):
        # On a SQLite-configured suite, every alias is sqlite → must
        # reject the 2PC ctx.
        from dorm.db.connection import get_connection
        from dorm.contrib.two_phase import two_phase_commit

        if getattr(get_connection(), "vendor", None) == "postgresql":
            pytest.skip("PG-only — covered by integration tests")
        with pytest.raises(NotImplementedError, match="PostgreSQL"):
            with two_phase_commit(["default"]):
                pass


# ── Inbox ──────────────────────────────────────────────────────────────────


class TestInboxDecorator:
    def test_decorator_attaches_handler_name(self):
        from dorm.contrib.inbox import InboxRecord, idempotent

        class _Inbox(InboxRecord):
            class Meta:
                app_label = "tests"

        calls: list[str] = []

        @idempotent(_Inbox, handler_name="test-handler")
        def handle(message_id: str, payload: dict) -> None:
            calls.append(message_id)

        # The decorator preserves the function signature.
        assert hasattr(handle, "__wrapped__")
        # Underlying function runnable without inbox plumbing.
        getattr(handle, "__wrapped__")("m1", {"x": 1})
        assert calls == ["m1"]
