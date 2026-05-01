"""N+1 query detector for dorm.

Wires into the :data:`dorm.signals.pre_query` signal, normalises every
SQL statement that runs inside the context to a parameter-stripped
template, and counts how many times each template fires. When a
template's count crosses ``threshold`` (default 5) the detector
either raises :class:`NPlusOneError` (the strict mode used in tests)
or accumulates findings for later inspection.

Two entry points:

* :class:`NPlusOneDetector` — context manager. Wrap any block of code::

    from dorm.contrib.nplusone import NPlusOneDetector

    with NPlusOneDetector():
        for author in Author.objects.all():
            print(author.publisher.name)        # raises — N descriptor reads
        # …unless you add ``select_related("publisher")`` first.

* :func:`assert_no_nplusone` — pytest helper. Same job as the context
  manager but raises an assertion error inside a test (clearer pytest
  output than a ``RuntimeError``).

The detector only catches **repeated** queries of the same shape. A
loop that issues 1000 *different* queries (e.g. ``filter(name=x)``
where ``x`` differs each time) is not an N+1 — it's by-design fan-out
on distinct keys, and the parameter-stripping is what tells the two
apart. ``filter(pk=$VAR)`` repeated 1000 times collapses to one
template after stripping; ``filter(name=...)`` with different
generated SQL stays distinct.

Implementation note: the signal hook captures the SQL exactly as
emitted, including ``%s`` / ``$1`` placeholders. We replace string /
numeric / NULL **literals** in the body with ``?``; placeholders are
already parameter-shaped and survive untouched. This keeps templates
stable across SQLite (``%s``) and PostgreSQL (``$1``) backends.
"""

from __future__ import annotations

import re
import threading
from contextlib import contextmanager
from typing import Any, Iterator

from ..signals import pre_query


class NPlusOneError(AssertionError):
    """Raised when a SQL template runs more than ``threshold`` times
    inside a :class:`NPlusOneDetector` block.

    Inherits from :class:`AssertionError` so pytest's traceback
    rewriting renders it nicely and ``-x`` / ``--maxfail=1`` treats
    it like a regular assertion failure.
    """


# Strip everything that looks like a literal value from a SQL string,
# leaving placeholders intact. Order matters — quoted strings first
# (so a number inside a string isn't misclassified), then numeric
# literals, then bare NULL. Patterns are deliberately permissive: a
# false positive (collapsing two distinct templates into one) only
# inflates the count and produces a louder report; a false negative
# (failing to collapse semantically identical templates) just hides
# a real N+1, which is the tradeoff we lean against.
_STRIP_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    # Single-quoted strings ARE literals in standard SQL (and in
    # SQLite + PostgreSQL specifically). Double-quoted tokens, in
    # contrast, are *identifiers* (table / column names) — never
    # collapse those: doing so would erase the structural shape we
    # rely on to tell two unrelated queries apart, plus break the
    # tests that pattern-match column names in the output.
    (re.compile(r"'(?:[^']|'')*'"), "?"),
    # Hex literals (``0xDEADBEEF``) and PG byte strings
    # (``X'…'`` / ``B'…'`` / ``E'…'``) — both backends accept
    # these. Order matters: must come before the bare-numeric
    # pattern so ``0xABCD`` isn't truncated to ``0x?CD``.
    (re.compile(r"0[xX][0-9a-fA-F]+"), "?"),
    (re.compile(r"[XBExbe]'(?:[^']|'')*'"), "?"),
    # Numbers — leading sign, decimal, scientific notation. The
    # previous ``\b\d+(?:\.\d+)?\b`` missed negatives (kept the
    # leading ``-`` outside the placeholder so ``= -5`` and
    # ``= 5`` produced different templates) and ate the mantissa
    # of scientific literals (``1.5e10`` → ``?.5e10``). The
    # tightened pattern handles all three.
    (re.compile(r"(?<![A-Za-z_0-9])-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?"), "?"),
    (re.compile(r"\bNULL\b", re.IGNORECASE), "?"),
]


def _normalize(sql: str) -> str:
    """Reduce *sql* to a param-stripped template suitable for grouping.

    The template preserves the SQL structure (table names, column
    names, JOINs, WHERE clause shape) so semantically equivalent
    queries with different bound values map to the same key.
    """
    out = sql
    for pat, repl in _STRIP_PATTERNS:
        out = pat.sub(repl, out)
    # Collapse whitespace so cosmetic formatting differences don't
    # split otherwise-identical templates.
    out = re.sub(r"\s+", " ", out).strip()
    return out


class NPlusOneDetector:
    """Context manager that watches every query emitted in its block
    and flags templates that fire more than *threshold* times.

    Args:
        threshold: A template that runs more than this many times is
            considered a violation. Default 5 — generous enough that
            a small loop over a fixed dataset doesn't trip it, tight
            enough that a 100-row N+1 fires.
        raise_on_detect: If True (default in tests), raise
            :class:`NPlusOneError` the first time a template crosses
            the threshold. If False, accumulate every offender in
            :attr:`findings` for later inspection (useful in dev /
            staging where you want a report, not a hard fail).
        ignore: Iterable of SQL substrings to skip. Useful for
            silencing ``CREATE TABLE`` / ``PRAGMA`` noise emitted by
            the dorm test harness; queries containing any substring
            on this list are not counted.
    """

    DEFAULT_IGNORE: tuple[str, ...] = (
        "CREATE TABLE",
        "DROP TABLE",
        "ALTER TABLE",
        "PRAGMA",
        "BEGIN",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT",
        "RELEASE",
    )

    def __init__(
        self,
        threshold: int = 5,
        *,
        raise_on_detect: bool = True,
        ignore: tuple[str, ...] = DEFAULT_IGNORE,
    ) -> None:
        if threshold < 1:
            raise ValueError("threshold must be >= 1")
        self.threshold = threshold
        self.raise_on_detect = raise_on_detect
        # Substrings checked case-insensitively against each SQL
        # statement; storing the upper-cased versions once is
        # marginally faster than re-uppercasing on every signal.
        self._ignore = tuple(s.upper() for s in ignore)
        self.counts: dict[str, int] = {}
        self.findings: list[tuple[str, int]] = []
        # ``dispatch_uid`` makes the signal connect / disconnect
        # idempotent across nested ``with`` blocks; we vary it per
        # instance so two parallel detectors don't share state.
        self._uid = f"nplusone-{id(self)}-{threading.get_ident()}"
        self._reported: set[str] = set()

    def _on_query(self, sender: Any, sql: str, params: Any, **kwargs: Any) -> None:
        upper = sql.lstrip().upper()
        if any(s in upper for s in self._ignore):
            return
        template = _normalize(sql)
        n = self.counts.get(template, 0) + 1
        self.counts[template] = n
        # First crossing of the threshold becomes a finding. Raising
        # straight from the signal handler is a non-starter: dorm's
        # :class:`Signal` swallows + logs receiver exceptions (so a
        # broken third-party hook can't take down the request),
        # which means a synchronous raise here never reaches the
        # caller. Findings are aggregated and re-raised by
        # :meth:`__exit__` instead.
        if n > self.threshold and template not in self._reported:
            self._reported.add(template)
            self.findings.append((template, n))

    def __enter__(self) -> "NPlusOneDetector":
        pre_query.connect(self._on_query, weak=False, dispatch_uid=self._uid)
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        pre_query.disconnect(dispatch_uid=self._uid)
        # Don't pile a second exception on top of one already in flight
        # — the user's exception is the more useful signal.
        if exc_type is not None:
            return
        if self.raise_on_detect and self.findings:
            tmpl, count = self.findings[0]
            raise NPlusOneError(
                f"N+1 detected: same query ran {count} times "
                f"(threshold {self.threshold}). Template:\n  {tmpl}"
            )

    def report(self) -> str:
        """Render the accumulated findings as a human-readable string.

        Lists every template that crossed the threshold, sorted by
        descending count. Empty when no violation tripped (or when the
        detector ran in ``raise_on_detect=True`` mode and the first
        offender raised before more could pile up).
        """
        if not self.findings:
            return "no N+1 detected"
        lines = [f"{n} executions: {tmpl}" for tmpl, n in sorted(
            self.findings, key=lambda t: -t[1]
        )]
        return "\n".join(lines)


@contextmanager
def assert_no_nplusone(threshold: int = 5) -> Iterator[NPlusOneDetector]:
    """Pytest-style helper: ``with assert_no_nplusone(): ...``.

    Wraps :class:`NPlusOneDetector` with ``raise_on_detect=True`` so
    the first offender bubbles up as :class:`NPlusOneError` (an
    ``AssertionError`` subclass). Yields the detector so the test can
    inspect ``.counts`` afterwards if it wants to assert on specific
    templates.
    """
    detector = NPlusOneDetector(threshold=threshold, raise_on_detect=True)
    with detector:
        yield detector


__all__ = [
    "NPlusOneDetector",
    "NPlusOneError",
    "assert_no_nplusone",
]
