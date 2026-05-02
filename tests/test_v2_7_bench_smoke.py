"""Smoke test for the ``bench.run`` benchmark runner.

We run the runner end-to-end against a tiny SQLite file with the
smallest possible parameters. The goal is to keep the runner
honest — every scenario survives a real round-trip — without
turning a benchmark suite into a regular CI cost driver.

Numbers are NOT asserted; only the JSON shape and the absence of
exceptions.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile

import pytest


@pytest.mark.timeout(60)
def test_bench_runner_smoke():
    """``python -m bench.run --backend sqlite --runs 1 --ops 5``
    must produce a valid JSON summary with a key per scenario."""
    with tempfile.TemporaryDirectory() as tmp:
        out = os.path.join(tmp, "result.json")
        # Use a fresh subprocess so the runner's ``dorm.configure(...)``
        # call doesn't clash with the conftest's session fixture.
        env = dict(os.environ)
        env.pop("DORM_SETTINGS", None)
        result = subprocess.run(
            [sys.executable, "-m", "bench.run",
             "--backend", "sqlite",
             "--runs", "1",
             "--ops", "5",
             "--output", out],
            capture_output=True,
            text=True,
            env=env,
            check=False,
            cwd=os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        )
        assert result.returncode == 0, (
            f"bench runner exited {result.returncode}: stdout={result.stdout!r} "
            f"stderr={result.stderr!r}"
        )

        with open(out, encoding="utf-8") as fh:
            summary = json.load(fh)
        assert summary["backend"] == "sqlite"
        # Every scenario the runner advertises must produce a number.
        for name in ("create", "bulk_create", "get", "filter_count", "list_first_n"):
            assert name in summary["median_seconds_per_op"], name
            v = summary["median_seconds_per_op"][name]
            assert isinstance(v, (int, float)) and v >= 0
