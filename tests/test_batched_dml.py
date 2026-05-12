"""Tests for ``QuerySet.delete_batched`` + ``update_batched``."""
from __future__ import annotations

import pytest

from tests.models import Author


class TestDeleteBatched:
    def test_deletes_all_rows_in_chunks(self):
        for i in range(25):
            Author.objects.create(name=f"a{i}", age=20)
        assert Author.objects.count() == 25
        # Small batch — forces multiple iterations.
        total = Author.objects.delete_batched(batch_size=7)
        assert total == 25
        assert Author.objects.count() == 0

    def test_respects_filter(self):
        for i in range(10):
            Author.objects.create(name=f"keep{i}", age=20)
        for i in range(10):
            Author.objects.create(name=f"drop{i}", age=20)
        total = Author.objects.filter(name__startswith="drop").delete_batched(
            batch_size=4
        )
        assert total == 10
        assert Author.objects.count() == 10
        assert all(a.name.startswith("keep") for a in Author.objects.all())

    def test_no_rows_returns_zero(self):
        # Empty table — no iterations should occur.
        assert Author.objects.count() == 0
        assert Author.objects.delete_batched() == 0

    def test_invalid_batch_size_rejected(self):
        with pytest.raises(ValueError, match="batch_size"):
            Author.objects.delete_batched(batch_size=0)


class TestUpdateBatched:
    def test_updates_all_rows_in_chunks(self):
        for i in range(15):
            Author.objects.create(name=f"x{i}", age=20)
        n = Author.objects.update_batched(batch_size=4, name="UPDATED")
        assert n == 15
        assert all(a.name == "UPDATED" for a in Author.objects.all())

    def test_no_kwargs_returns_zero(self):
        Author.objects.create(name="a", age=20)
        assert Author.objects.update_batched(batch_size=10) == 0

    def test_respects_filter(self):
        Author.objects.create(name="alpha", age=20)
        Author.objects.create(name="alpha", age=20)
        Author.objects.create(name="beta", age=20)
        n = Author.objects.filter(name="alpha").update_batched(
            batch_size=1, name="gamma"
        )
        assert n == 2
        assert Author.objects.filter(name="gamma").count() == 2
        assert Author.objects.filter(name="beta").count() == 1

    def test_invalid_batch_size_rejected(self):
        with pytest.raises(ValueError, match="batch_size"):
            Author.objects.update_batched(batch_size=0, name="x")
