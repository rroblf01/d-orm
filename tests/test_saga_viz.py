"""Tests for ``Saga.to_mermaid`` / ``Saga.to_dot`` + the
``dorm saga-graph`` CLI command."""
from __future__ import annotations

import argparse
import sys

import pytest

from dorm.contrib.saga import Saga, Step
from dorm.contrib.saga import _safe_id


def _saga():
    """Three-step saga: A → B → C. Step ``B`` has no compensation
    on purpose so the renderer's ``non_comp`` styling is exercised."""
    return Saga(
        steps=[
            Step(name="reserve", forward=lambda ctx: None, compensate=lambda ctx: None),
            Step(name="charge",  forward=lambda ctx: None),
            Step(name="ship",    forward=lambda ctx: None, compensate=lambda ctx: None),
        ]
    )


class TestMermaidRenderer:
    def test_emits_header_and_nodes(self):
        out = _saga().to_mermaid()
        assert out.splitlines()[0] == "graph LR"
        for name in ("reserve", "charge", "ship"):
            assert f'"{name}"' in out

    def test_forward_edges_solid(self):
        out = _saga().to_mermaid()
        assert "reserve --> charge" in out
        assert "charge --> ship" in out

    def test_compensation_edges_dotted_only_when_compensable(self):
        out = _saga().to_mermaid()
        # Only ``ship`` (compensable) emits a back-edge to its predecessor.
        assert "ship -.compensate.-> charge" in out
        # ``charge`` has no compensation — no back-edge to reserve.
        assert "charge -.compensate.-> reserve" not in out

    def test_non_comp_class_applied(self):
        out = _saga().to_mermaid()
        # ``charge`` has no compensate — must get the ``non_comp`` class.
        assert "class charge non_comp" in out
        assert "classDef non_comp" in out

    def test_title_emitted_as_comment(self):
        out = _saga().to_mermaid(title="Order pipeline")
        assert out.startswith("%% Order pipeline")


class TestDotRenderer:
    def test_emits_digraph_header(self):
        out = _saga().to_dot()
        assert out.startswith("digraph Saga {")
        assert out.rstrip().endswith("}")
        assert "rankdir=LR;" in out

    def test_forward_edges(self):
        out = _saga().to_dot()
        assert "reserve -> charge;" in out
        assert "charge -> ship;" in out

    def test_compensation_dashed(self):
        out = _saga().to_dot()
        assert (
            'ship -> charge [style="dashed", label="compensate"];' in out
        )

    def test_non_comp_step_styled_red(self):
        out = _saga().to_dot()
        # ``charge`` is non-compensable.
        assert 'color="red"' in out

    def test_title_emitted(self):
        out = _saga().to_dot(title="Order pipeline")
        assert 'label="Order pipeline"' in out


class TestSafeId:
    def test_replaces_punctuation(self):
        assert _safe_id("foo bar") == "foo_bar"
        assert _safe_id("a-b.c") == "a_b_c"

    def test_prefixes_when_starts_with_digit(self):
        assert _safe_id("9steps").startswith("n_")


class TestSagaGraphCLI:
    def test_unknown_path_exits(self, capsys):
        from dorm.cli import cmd_saga_graph

        with pytest.raises(SystemExit):
            cmd_saga_graph(
                argparse.Namespace(
                    path="not.a.real.module", format="mermaid", title=None,
                )
            )
        err = capsys.readouterr().err
        assert "Error importing" in err

    def test_resolved_object_not_a_saga(self, tmp_path, monkeypatch, capsys):
        from dorm.cli import cmd_saga_graph

        # Write a tmp module exposing a non-Saga attribute.
        (tmp_path / "_saga_fixture.py").write_text(
            "value = 'not-a-saga'\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        sys.modules.pop("_saga_fixture", None)
        try:
            with pytest.raises(SystemExit):
                cmd_saga_graph(
                    argparse.Namespace(
                        path="_saga_fixture.value", format="mermaid", title=None,
                    )
                )
            err = capsys.readouterr().err
            assert "not a Saga" in err
        finally:
            sys.modules.pop("_saga_fixture", None)

    def test_invalid_path_shape_exits(self, capsys):
        from dorm.cli import cmd_saga_graph

        with pytest.raises(SystemExit):
            cmd_saga_graph(
                argparse.Namespace(path="nope", format="mermaid", title=None)
            )
        err = capsys.readouterr().err
        assert "must be" in err

    def test_renders_real_saga(self, tmp_path, monkeypatch, capsys):
        from dorm.cli import cmd_saga_graph

        (tmp_path / "_saga_real.py").write_text(
            "from dorm.contrib.saga import Saga, Step\n"
            "saga = Saga(steps=[\n"
            "    Step(name='a', forward=lambda ctx: None, compensate=lambda ctx: None),\n"
            "    Step(name='b', forward=lambda ctx: None),\n"
            "])\n"
        )
        monkeypatch.syspath_prepend(str(tmp_path))
        sys.modules.pop("_saga_real", None)
        try:
            cmd_saga_graph(
                argparse.Namespace(
                    path="_saga_real:saga", format="mermaid", title=None,
                )
            )
            out = capsys.readouterr().out
            assert "graph LR" in out
            assert '"a"' in out
        finally:
            sys.modules.pop("_saga_real", None)
