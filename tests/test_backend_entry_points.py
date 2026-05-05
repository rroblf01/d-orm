"""Tests for the third-party backend entry-point loader.

We register a fake backend via ``monkeypatch`` so the test does not
rely on a real installed package. The test verifies that:

- ``ENGINE = '<custom>'`` looks the engine up in the entry-point group
- The cache is populated lazily and survives subsequent calls
- ``reset_backend_cache()`` clears it
"""

from __future__ import annotations

import pytest

from dorm.db import connection as conn_mod
from dorm.exceptions import ImproperlyConfigured


class _FakeWrapper:
    vendor = "fake"

    def __init__(self, settings):
        self.settings = settings


def _wrapper_factory(settings):
    return _FakeWrapper(settings)


class _FakeEntryPoint:
    """Stand-in for ``importlib.metadata.EntryPoint`` — the real class
    became immutable in Python 3.14, so we hand the loader a lookalike
    duck-typed to the only methods it touches (``name`` + ``load``)."""

    def __init__(self, name: str, factory):
        self.name = name
        self._factory = factory

    def load(self):
        return self._factory


@pytest.fixture
def fake_backend(monkeypatch):
    """Patch entry_points() to advertise a single fake backend."""
    ep = _FakeEntryPoint("fake", _wrapper_factory)

    def _fake_entry_points(*, group=None, **_kw):
        if group == "djanorm.backends":
            return (ep,)
        return ()

    monkeypatch.setattr(conn_mod, "_BACKEND_CACHE", None)
    monkeypatch.setattr(conn_mod, "_ASYNC_BACKEND_CACHE", None)
    import importlib.metadata as md

    monkeypatch.setattr(md, "entry_points", _fake_entry_points)
    yield
    conn_mod.reset_backend_cache()


def test_unknown_engine_lists_supported(monkeypatch):
    monkeypatch.setattr(conn_mod, "_BACKEND_CACHE", None)
    with pytest.raises(ImproperlyConfigured, match="Supported"):
        conn_mod._create_sync_connection("default", {"ENGINE": "totally-bogus"})


def test_entry_point_backend_loaded(fake_backend):
    obj = conn_mod._create_sync_connection("default", {"ENGINE": "fake"})
    assert isinstance(obj, _FakeWrapper)


def test_entry_point_cache_reused(fake_backend):
    obj1 = conn_mod._create_sync_connection("default", {"ENGINE": "fake"})
    obj2 = conn_mod._create_sync_connection("default", {"ENGINE": "fake"})
    assert isinstance(obj1, _FakeWrapper)
    assert isinstance(obj2, _FakeWrapper)
    # Cache populated.
    assert conn_mod._BACKEND_CACHE is not None
    assert "fake" in conn_mod._BACKEND_CACHE


def test_reset_backend_cache():
    conn_mod._BACKEND_CACHE = {"x": object()}
    conn_mod._ASYNC_BACKEND_CACHE = {"x": object()}
    conn_mod.reset_backend_cache()
    assert conn_mod._BACKEND_CACHE is None
    assert conn_mod._ASYNC_BACKEND_CACHE is None
