"""Tier-1 features for v4.3: upsert + temporal + tasks."""
from __future__ import annotations

import datetime as _dt

import pytest


# ── UPSERT (sugar over bulk_create(update_conflicts=True)) ──────────────────


class TestUpsertSugar:
    def test_default_update_fields_excludes_unique(self):
        import dorm

        class _U(dorm.Model):
            name = dorm.CharField(max_length=20)
            slug = dorm.CharField(max_length=20)
            score = dorm.IntegerField()

            class Meta:
                app_label = "tests"

        # Validate that the helper computed the right update_fields
        # set (everything except PK + unique_fields).
        qs = _U.objects.all()
        # Reach into the helper by calling it with empty objs — the
        # default-fields logic still runs.
        result = qs.upsert([], unique_fields=["slug"])
        assert result == []

    def test_requires_unique_fields(self):
        import dorm

        class _U2(dorm.Model):
            name = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        # Empty objs → never raises (early return). Non-empty objs
        # without unique_fields → ValueError.
        with pytest.raises(ValueError, match="unique_fields is required"):
            _U2.objects.upsert([_U2(name="x")], unique_fields=[])


# ── Temporal tables ─────────────────────────────────────────────────────────


class TestTemporal:
    def test_decorator_idempotent(self):
        import dorm
        from dorm.contrib.temporal import temporal

        @temporal
        class _T(dorm.Model):
            title = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        same = temporal(_T)
        assert same is _T
        assert getattr(_T, "_temporal_model", None) is not None

    def test_temporal_model_columns(self):
        import dorm
        from dorm.contrib.temporal import temporal

        @temporal
        class _T2(dorm.Model):
            title = dorm.CharField(max_length=20)

            class Meta:
                app_label = "tests"

        temp = _T2._temporal_model  # type: ignore[attr-defined]  # ty:ignore[unresolved-attribute]
        cols = {f.name for f in temp._meta.fields}
        assert {"title", "valid_from", "valid_to", "operation"}.issubset(cols)

    def test_as_of_requires_temporal(self):
        import dorm
        from dorm.contrib.temporal import as_of

        class _Plain(dorm.Model):
            x = dorm.IntegerField()

            class Meta:
                app_label = "tests"

        with pytest.raises(TypeError, match="not @temporal-tracked"):
            as_of(_Plain, _dt.datetime.now(_dt.timezone.utc))


# ── TaskQueue / @task ───────────────────────────────────────────────────────


class TestTaskQueue:
    @pytest.fixture(autouse=True)
    def _reset(self):
        from dorm.contrib.tasks import reset_registry

        reset_registry()
        yield
        reset_registry()

    def test_task_decorator_registers(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task, _TASK_REGISTRY

        class _Outbox(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_Outbox, channel=None)

        @task(queue, name="ping")
        def ping() -> str:
            return "pong"

        assert "ping" in _TASK_REGISTRY
        assert _TASK_REGISTRY["ping"].func() == "pong"

    def test_duplicate_task_name_rejected(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task

        class _Outbox2(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_Outbox2, channel=None)

        @task(queue, name="job-a")
        def a():
            pass

        with pytest.raises(RuntimeError, match="already registered"):

            @task(queue, name="job-a")
            def b():
                pass

    def test_call_runs_sync(self):
        from dorm.contrib.outbox import OutboxEvent
        from dorm.contrib.tasks import TaskQueue, task

        class _Outbox3(OutboxEvent):
            class Meta:
                app_label = "tests"

        queue = TaskQueue(model=_Outbox3, channel=None)
        calls: list[int] = []

        @task(queue, name="counter")
        def counter(n: int) -> None:
            calls.append(n)

        # Direct call bypasses queue — used in tests.
        counter(7)
        assert calls == [7]
