"""Tests for the ``dorm models-tree`` CLI command."""
from __future__ import annotations

import argparse


class TestModelsTree:
    def test_prints_registered_models_and_edges(self, capsys):
        from dorm.cli import cmd_models_tree

        # The ``tests`` app is registered via conftest; models include
        # Article (FK author, FK publisher) + Tag M2M etc.
        cmd_models_tree(argparse.Namespace(settings=None))
        out = capsys.readouterr().out
        # App header line.
        assert "tests/" in out
        # At least one model from the app.
        assert "Author" in out
        # And at least one FK edge — Article has FK author + publisher.
        assert "FK" in out
        # Arrow rendering for relations.
        assert "→" in out

    def test_no_models_registered_path(self, monkeypatch, capsys):
        from dorm.cli import cmd_models_tree
        from dorm.models import _model_registry

        # Swap registry with an empty dict so the helper hits its
        # "(no models registered)" branch — restore after.
        saved = dict(_model_registry)
        _model_registry.clear()
        try:
            cmd_models_tree(argparse.Namespace(settings=None))
            out = capsys.readouterr().out
            assert "(no models registered)" in out
        finally:
            _model_registry.update(saved)
