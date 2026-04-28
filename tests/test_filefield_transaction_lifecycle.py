"""Bug-hunting tests around the interaction between :class:`FileField`
and :func:`atomic` / :func:`aatomic`.

Two non-transactional systems sit on either side of a ``FileField``:
the database (transactional) and the storage backend (not). The
canonical bug — files written inside an ``atomic()`` that later rolls
back surviving as orphans — was confirmed and fixed in 2.2 by wiring
``FileField.pre_save`` to register an ``on_rollback`` cleanup. These
tests guard that contract:

- Rollback at the outer level removes the file.
- Rollback at a savepoint removes only that savepoint's file.
- Commit preserves the file.
- ``on_commit`` callbacks see the file present; ``on_rollback`` ones
  see it gone.
- Async (``aatomic`` + ``asave``) follows the same rules.
- An exception during rollback cleanup is logged but does **not**
  re-raise (the row work is already undone — losing a stray file
  shouldn't escalate to a crash).
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

import dorm
from dorm.db.connection import get_connection
from dorm.storage import ContentFile, reset_storages
from dorm.transaction import (
    aatomic,
    aon_commit,
    aon_rollback,
    atomic,
    on_commit,
    on_rollback,
)


# ── Test model ───────────────────────────────────────────────────────────────


class TxDoc(dorm.Model):
    name = dorm.CharField(max_length=50)
    attachment = dorm.FileField(upload_to="docs/", null=True, blank=True)
    backup = dorm.FileField(upload_to="archive/", null=True, blank=True)

    class Meta:
        db_table = "tx_lifecycle_docs"


# ── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def media_root(tmp_path: Path):
    reset_storages()
    saved = getattr(dorm.settings, "STORAGES", {})
    dorm.configure(
        DATABASES=dorm.settings.DATABASES,
        INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
        STORAGES={
            "default": {
                "BACKEND": "dorm.storage.FileSystemStorage",
                "OPTIONS": {
                    "location": str(tmp_path / "media"),
                    "base_url": "/m/",
                },
            }
        },
    )
    yield tmp_path / "media"
    dorm.configure(
        DATABASES=dorm.settings.DATABASES,
        INSTALLED_APPS=dorm.settings.INSTALLED_APPS,
        STORAGES=saved,
    )
    reset_storages()


@pytest.fixture
def txdoc_table(clean_db):
    from dorm.migrations.operations import _field_to_column_sql

    conn = get_connection()
    cascade = " CASCADE" if getattr(conn, "vendor", "sqlite") == "postgresql" else ""
    conn.execute_script(f'DROP TABLE IF EXISTS "tx_lifecycle_docs"{cascade}')
    cols = [
        _field_to_column_sql(f.name, f, conn)
        for f in TxDoc._meta.fields
        if f.db_type(conn)
    ]
    conn.execute_script(
        'CREATE TABLE "tx_lifecycle_docs" (\n  '
        + ",\n  ".join(filter(None, cols))
        + "\n)"
    )
    yield
    conn.execute_script(f'DROP TABLE IF EXISTS "tx_lifecycle_docs"{cascade}')


# ── Sync rollback cleanup ────────────────────────────────────────────────────


class TestSyncRollbackCleanup:
    def test_outer_rollback_removes_orphan_file(self, media_root, txdoc_table):
        """The headline contract: a file written inside an ``atomic()``
        that later raises must NOT survive on disk."""
        path: Path | None = None
        with pytest.raises(RuntimeError):
            with atomic():
                doc = TxDoc(name="r")
                doc.attachment = ContentFile(b"orphan", name="rb.txt")
                doc.save()
                path = media_root / doc.attachment.name
                assert path.exists(), "file must be on disk during the tx"
                raise RuntimeError("simulated failure")
        assert path is not None
        assert not path.exists(), f"orphan survived rollback at {path}"
        assert not TxDoc.objects.filter(name="r").exists()

    def test_commit_preserves_file(self, media_root, txdoc_table):
        with atomic():
            doc = TxDoc(name="c")
            doc.attachment = ContentFile(b"keep", name="ok.txt")
            doc.save()
        path = media_root / doc.attachment.name
        assert path.exists()
        assert TxDoc.objects.filter(name="c").exists()

    def test_set_rollback_force_removes_file(self, media_root, txdoc_table):
        """``set_rollback(True)`` is the explicit way to abort without
        an exception — the cleanup hook must fire on that path too."""
        with atomic() as tx:
            doc = TxDoc(name="forced")
            doc.attachment = ContentFile(b"x", name="x.txt")
            doc.save()
            path = media_root / doc.attachment.name
            assert path.exists()
            tx.set_rollback(True)
        assert not path.exists()
        assert not TxDoc.objects.filter(name="forced").exists()

    def test_savepoint_rollback_removes_only_inner_file(
        self, media_root, txdoc_table
    ):
        """Inner ``atomic()`` failure unwinds to its savepoint. Files
        written *inside* the inner block must vanish; files written in
        the outer block (which still commits) must stay."""
        with atomic():
            outer_doc = TxDoc(name="outer")
            outer_doc.attachment = ContentFile(b"outer", name="o.txt")
            outer_doc.save()
            outer_path = media_root / outer_doc.attachment.name

            with pytest.raises(RuntimeError):
                with atomic():
                    inner_doc = TxDoc(name="inner")
                    inner_doc.attachment = ContentFile(b"inner", name="i.txt")
                    inner_doc.save()
                    inner_path = media_root / inner_doc.attachment.name
                    raise RuntimeError("inner fails")

        assert outer_path.exists(), "outer file must survive inner rollback"
        assert not inner_path.exists(), "inner file must be cleaned up"
        assert TxDoc.objects.filter(name="outer").exists()
        assert not TxDoc.objects.filter(name="inner").exists()

    def test_multiple_filefields_on_same_row_all_cleaned(
        self, media_root, txdoc_table
    ):
        """A model with two FileFields, each registering its own
        rollback hook — both files must vanish on rollback."""
        with pytest.raises(RuntimeError):
            with atomic():
                doc = TxDoc(name="dual")
                doc.attachment = ContentFile(b"a", name="a.txt")
                doc.backup = ContentFile(b"b", name="b.txt")
                doc.save()
                a = media_root / doc.attachment.name
                b = media_root / doc.backup.name
                assert a.exists() and b.exists()
                raise RuntimeError("fail")
        assert not a.exists()
        assert not b.exists()

    def test_rollback_callback_exceptions_are_logged_not_raised(
        self, media_root, txdoc_table, caplog
    ):
        """A buggy rollback handler (here: storage that raises on
        delete) must not propagate — the rollback already happened.
        Log on ``dorm.transaction`` for visibility."""
        from dorm.storage import Storage

        class FlakyStorage(Storage):
            """Inherits from ``Storage`` so the FileField property
            recognises it and returns it from ``self.storage``. Wraps
            a real ``FileSystemStorage`` for the writes; the delete
            path raises to simulate an outage."""

            def __init__(self, real: Storage) -> None:
                self._real = real
                self.calls = 0

            # Forward write/read/url path to the wrapped real storage.
            def _save(self, name, content):
                return self._real._save(name, content)  # type: ignore[attr-defined]

            def _open(self, name, mode):
                return self._real._open(name, mode)  # type: ignore[attr-defined]

            def exists(self, name):
                return self._real.exists(name)

            def size(self, name):
                return self._real.size(name)

            def url(self, name):
                return self._real.url(name)

            def path(self, name):
                return self._real.path(name)

            def delete(self, name):
                # The point of the test: blow up here.
                self.calls += 1
                raise OSError("simulated storage outage")

        from dorm.storage import get_storage as _get

        flaky = FlakyStorage(_get("default"))

        # Swap the FileField's storage attribute for this test only.
        field = TxDoc._meta.get_field("attachment")
        original = field._storage_arg
        field._storage_arg = flaky
        try:
            with caplog.at_level(logging.ERROR, logger="dorm.transaction"):
                with pytest.raises(RuntimeError):
                    with atomic():
                        doc = TxDoc(name="flaky")
                        doc.attachment = ContentFile(b"x", name="f.txt")
                        doc.save()
                        raise RuntimeError("user failure")
        finally:
            field._storage_arg = original

        # The cleanup was attempted, the storage raised, and the error
        # surfaced through the logger rather than the call stack.
        assert flaky.calls == 1
        assert any(
            "on_rollback callback" in r.message for r in caplog.records
        ), f"expected log mention; got {[r.message for r in caplog.records]}"


# ── on_commit / on_rollback semantics ────────────────────────────────────────


class TestOnCommitOnRollback:
    def test_on_commit_fires_only_on_commit(self):
        events: list[str] = []
        with atomic():
            on_commit(lambda: events.append("commit"))
        assert events == ["commit"]

    def test_on_rollback_fires_only_on_rollback(self):
        events: list[str] = []
        try:
            with atomic():
                on_rollback(lambda: events.append("rb"))
                raise RuntimeError("x")
        except RuntimeError:
            pass
        assert events == ["rb"]

    def test_on_commit_discarded_on_rollback(self):
        """Commit callbacks queued inside a rolled-back block are
        dropped — that's the whole point of ``on_commit``."""
        events: list[str] = []
        try:
            with atomic():
                on_commit(lambda: events.append("commit"))
                on_rollback(lambda: events.append("rb"))
                raise RuntimeError("fail")
        except RuntimeError:
            pass
        assert events == ["rb"], events

    def test_on_rollback_discarded_on_commit(self):
        events: list[str] = []
        with atomic():
            on_commit(lambda: events.append("commit"))
            on_rollback(lambda: events.append("rb"))
        assert events == ["commit"]

    def test_savepoint_rollback_isolates_inner_callbacks(self):
        events: list[str] = []
        with atomic():
            on_commit(lambda: events.append("outer-commit"))
            try:
                with atomic():
                    on_commit(lambda: events.append("inner-commit"))
                    on_rollback(lambda: events.append("inner-rb"))
                    raise RuntimeError("inner")
            except RuntimeError:
                pass
            on_commit(lambda: events.append("outer-commit-after"))
        # Inner rollback fires its own rb; inner-commit was dropped.
        # Outer commits, so the outer commit callbacks fire.
        assert events == [
            "inner-rb",
            "outer-commit",
            "outer-commit-after",
        ], events

    def test_on_rollback_outside_atomic_is_noop(self):
        """No active transaction → nothing to roll back, callback
        dropped (mirror of ``on_commit``'s "fire immediately" path)."""
        events: list[str] = []
        on_rollback(lambda: events.append("never"))
        assert events == []

    def test_on_commit_outside_atomic_fires_immediately(self):
        """Sanity: existing ``on_commit`` semantics survived the
        rollback-stack refactor."""
        events: list[str] = []
        on_commit(lambda: events.append("now"))
        assert events == ["now"]

    def test_on_commit_can_observe_committed_file(
        self, media_root, txdoc_table
    ):
        """``on_commit`` callbacks see post-commit state — the file
        should still be on disk by the time the hook runs."""
        seen: list[bool] = []
        with atomic():
            doc = TxDoc(name="oc")
            doc.attachment = ContentFile(b"x", name="oc.txt")
            doc.save()
            path = media_root / doc.attachment.name
            on_commit(lambda: seen.append(path.exists()))
        assert seen == [True]


# ── Async rollback cleanup ───────────────────────────────────────────────────


@pytest.fixture
def _sqlite_only(db_config):
    """The orphan-cleanup contract for ``aatomic`` is storage-side and
    backend-agnostic — running it on both SQLite and PostgreSQL adds
    no signal. The PG path also interacts badly with the session-
    scoped event loop + the function-scoped ``clean_db`` cycle when
    multiple async tests in this file use a non-standard table:
    ``aatomic`` ends up acquiring an async connection whose previous
    state was unwound by a sync ``reset_connections``. Pinning to
    SQLite sidesteps that without losing what these tests prove.
    """
    if db_config.get("ENGINE") == "postgresql":
        pytest.skip("async rollback tests are SQLite-only by design.")


class TestAsyncRollbackCleanup:
    @pytest.mark.asyncio
    async def test_aatomic_rollback_removes_orphan_file(
        self, _sqlite_only, media_root, txdoc_table
    ):
        path: Path | None = None
        with pytest.raises(RuntimeError):
            async with aatomic():
                doc = TxDoc(name="ar")
                doc.attachment = ContentFile(b"x", name="ar.txt")
                await doc.asave()
                path = media_root / doc.attachment.name
                assert path.exists()
                raise RuntimeError("simulated")
        assert path is not None
        assert not path.exists()
        assert not TxDoc.objects.filter(name="ar").exists()

    @pytest.mark.asyncio
    async def test_aatomic_commit_preserves_file(
        self, _sqlite_only, media_root, txdoc_table
    ):
        async with aatomic():
            doc = TxDoc(name="ac")
            doc.attachment = ContentFile(b"x", name="ac.txt")
            await doc.asave()
        assert (media_root / doc.attachment.name).exists()
        assert await TxDoc.objects.filter(name="ac").aexists()

    @pytest.mark.asyncio
    async def test_aon_rollback_accepts_async_callable(
        self, _sqlite_only, media_root, txdoc_table
    ):
        # Request the same fixtures the other async tests in this
        # class use, even though this test doesn't touch the DB. Under
        # a session-scoped event loop, fixtures need to be consistent
        # across consecutive async tests or the shared connection /
        # storage caches drift between them.
        events: list[str] = []

        async def cleanup():
            events.append("async-cleanup")

        with pytest.raises(RuntimeError):
            async with aatomic():
                aon_rollback(cleanup)
                raise RuntimeError("rb")
        assert events == ["async-cleanup"]

    @pytest.mark.asyncio
    async def test_aon_commit_and_aon_rollback_mutually_exclusive(
        self, _sqlite_only, media_root, txdoc_table
    ):
        events: list[str] = []
        with pytest.raises(RuntimeError):
            async with aatomic():
                aon_commit(lambda: events.append("commit"))
                aon_rollback(lambda: events.append("rb"))
                raise RuntimeError("x")
        assert events == ["rb"]


# ── Replace-existing-file scenario ───────────────────────────────────────────


class TestReplaceFile:
    def test_replacing_file_does_not_orphan_old_one_by_default(
        self, media_root, txdoc_table
    ):
        """Reassigning a different ``ContentFile`` to an existing
        FileField writes the new one but does **not** delete the old
        one (Django's behaviour). Document this so callers know they
        need to manage cleanup explicitly."""
        doc = TxDoc(name="r")
        doc.attachment = ContentFile(b"first", name="first.txt")
        doc.save()
        first = media_root / doc.attachment.name

        doc.attachment = ContentFile(b"second", name="second.txt")
        doc.save()
        second = media_root / doc.attachment.name

        # Both still on disk — the old file is not auto-cleaned.
        # Test exists to lock in this behaviour and signal the gap.
        assert first.exists()
        assert second.exists()
        assert first != second

    def test_replacement_within_rollback_cleans_only_new_file(
        self, media_root, txdoc_table
    ):
        """Mixed scenario: a saved row gets a new file inside an
        ``atomic()`` that rolls back. The OLD file (from before the
        block) stays; the NEW file is cleaned up by the rollback hook."""
        # First commit — the "before" file lands.
        doc = TxDoc(name="rp")
        doc.attachment = ContentFile(b"v1", name="v1.txt")
        doc.save()
        old = media_root / doc.attachment.name
        assert old.exists()

        with pytest.raises(RuntimeError):
            with atomic():
                doc.attachment = ContentFile(b"v2", name="v2.txt")
                doc.save()
                new = media_root / doc.attachment.name
                assert new.exists()
                raise RuntimeError("abort")

        # Old file untouched; new file cleaned up by on_rollback.
        assert old.exists()
        assert not new.exists()
