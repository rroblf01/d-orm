"""End-to-end test of the ``SELECT ... FOR UPDATE SKIP LOCKED`` pattern
as a job-queue worker primitive.

This is the canonical use case for ``skip_locked``: N workers compete
to claim rows from a queue table; each lands on a different row instead
of blocking. The test runs two workers in real threads against a real
PG instance and asserts they between them claim every row exactly once.

PostgreSQL-only — SQLite has neither row-level locking nor SKIP LOCKED.
"""

from __future__ import annotations

import threading

import pytest

from dorm import transaction
from tests.models import Author


def _is_postgres(db_config) -> bool:
    return db_config.get("ENGINE") == "postgresql"


def test_skip_locked_two_workers_claim_disjoint_rows(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")

    Author.objects.bulk_create(
        [Author(name=f"job-{i}", age=i) for i in range(20)]
    )

    claimed: dict[str, list[int]] = {"a": [], "b": []}
    barrier = threading.Barrier(2)

    def _worker(name: str) -> None:
        # Synchronise both threads at the start so they overlap.
        barrier.wait()
        with transaction.atomic():
            qs = (
                Author.objects.select_for_update(skip_locked=True)
                .order_by("pk")[:10]
            )
            for row in qs:
                claimed[name].append(row.pk)
                # Hold the transaction long enough that the other
                # worker reaches its own SELECT FOR UPDATE before we
                # commit; otherwise both workers serialise and one
                # claims everything.
            # Implicit commit on context-manager exit.

    t1 = threading.Thread(target=_worker, args=("a",))
    t2 = threading.Thread(target=_worker, args=("b",))
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    union = set(claimed["a"]) | set(claimed["b"])
    intersection = set(claimed["a"]) & set(claimed["b"])
    assert len(union) == 20, "every row should be claimed by exactly one worker"
    assert intersection == set(), "no row should be claimed twice"


def test_skip_locked_skips_locked_row(db_config):
    if not _is_postgres(db_config):
        pytest.skip("PG-only test")

    Author.objects.bulk_create([Author(name=f"r-{i}", age=i) for i in range(3)])

    blocker_holding = threading.Event()
    blocker_release = threading.Event()
    blocker_pks: list[int] = []

    def _blocker() -> None:
        with transaction.atomic():
            row = (
                Author.objects.select_for_update()
                .filter(name="r-0")
                .first()
            )
            assert row is not None
            blocker_pks.append(row.pk)
            blocker_holding.set()
            # Hold the lock until the main thread says we can release.
            blocker_release.wait(timeout=5.0)

    t = threading.Thread(target=_blocker)
    t.start()
    try:
        assert blocker_holding.wait(timeout=5.0)
        # The main thread asks for every row with SKIP LOCKED — the
        # locked one must be omitted.
        with transaction.atomic():
            visible = list(
                Author.objects.select_for_update(skip_locked=True).order_by("pk")
            )
        visible_pks = {row.pk for row in visible}
        assert blocker_pks[0] not in visible_pks
        assert len(visible_pks) == 2
    finally:
        blocker_release.set()
        t.join()
