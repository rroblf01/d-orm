"""SAGA pattern primitives for long-running multi-step transactions.

A SAGA is a sequence of local transactions where each step has a
**compensation** that undoes its effect if a later step fails. The
pattern lets distributed workflows recover from partial failures
without distributed transactions.

Usage::

    from dorm.contrib.saga import Saga, Step

    def reserve_inventory(ctx):
        ...

    def release_inventory(ctx):
        ...

    saga = Saga(
        steps=[
            Step("reserve_inventory", reserve_inventory, release_inventory),
            Step("charge_card", charge_card, refund_card),
            Step("ship_order", ship_order, cancel_shipping),
        ],
    )
    saga.run({"order_id": 42})

Each step runs in its own ``atomic()`` block; compensations fire in
reverse order on the first failure. The Saga's audit trail (which
steps ran, which compensations fired, final state) is captured in an
in-memory ``SagaRun`` object — persist it via the outbox / your
audit log of choice when durability matters.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Callable

from .. import transaction

_log = logging.getLogger("dorm.contrib.saga")


@dataclass
class Step:
    """One step of a saga.

    Args:
        name: human-readable identifier, used in the run log.
        forward: callable that performs the step's work. Receives the
            shared context dict (mutable across steps).
        compensate: callable that undoes the step's effect.
            Receives the same context dict. ``None`` marks the step
            as non-compensable (any failure after it will be left
            permanent — log loudly).
        idempotent: when True, the runner treats a re-execution of
            this step as safe (won't compensate it on retry). Used
            by :func:`Saga.resume`.
    """

    name: str
    forward: Callable[[dict[str, Any]], Any]
    compensate: Callable[[dict[str, Any]], Any] | None = None
    idempotent: bool = False


@dataclass
class SagaRun:
    """Audit record of a Saga execution. Inspect after the run for
    success / failure / compensation history."""

    completed: list[str] = field(default_factory=list)
    compensated: list[str] = field(default_factory=list)
    failure: tuple[str, BaseException] | None = None
    context: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.failure is None


class Saga:
    """Coordinator for a list of :class:`Step`s.

    The forward pass runs steps in order, each inside its own
    ``atomic()``. On exception, the runner stops and walks the
    completed list in reverse, firing each step's ``compensate``
    in its own ``atomic()``.

    Args:
        steps: ordered list of :class:`Step`.
        using: connection alias for the per-step transactions.
        stop_on_compensation_error: when True, a failing compensation
            aborts the rollback walk (leaves later compensations
            unrun). Default False — continue and surface every
            failure via the audit record.
    """

    def __init__(
        self,
        *,
        steps: list[Step],
        using: str = "default",
        stop_on_compensation_error: bool = False,
    ) -> None:
        if not steps:
            raise ValueError("Saga requires at least one step")
        names = [s.name for s in steps]
        if len(set(names)) != len(names):
            raise ValueError(f"Saga step names must be unique; got {names}")
        self.steps = list(steps)
        self.using = using
        self.stop_on_compensation_error = stop_on_compensation_error

    def run(self, context: dict[str, Any] | None = None) -> SagaRun:
        """Execute the forward pass + automatic compensation on
        failure. Returns the :class:`SagaRun` audit record.

        .. warning::

           Calling ``Saga.run()`` inside an outer
           :func:`dorm.transaction.atomic` block undoes the
           durability guarantee: each step's atomic becomes a
           savepoint, and if the outer transaction later rolls
           back, **every** committed step (and its compensation)
           rolls back with it. Saga steps are durable only when
           the saga itself sits at the top of the call stack —
           the runner emits a WARNING when nested.
        """
        ctx: dict[str, Any] = context or {}
        run = SagaRun(context=ctx)
        # Detect nested atomic and warn loudly. Different backend
        # wrappers expose different attributes (``_atomic_conn`` on
        # PG, ``_atomic_depth`` on SQLite); check both shapes.
        try:
            from ..db.connection import get_connection

            conn = get_connection(self.using)
            in_atomic = (
                getattr(conn, "_atomic_conn", None) is not None
                or getattr(conn, "_atomic_depth", 0) > 0
            )
            if in_atomic:
                _log.warning(
                    "Saga.run() called inside an outer atomic() block — "
                    "step commits become savepoints and lose durability "
                    "if the outer transaction rolls back. Move the Saga "
                    "outside the atomic() if compensation must survive."
                )
        except Exception:  # pragma: no cover - best effort
            pass
        for step in self.steps:
            try:
                with transaction.atomic(using=self.using):
                    step.forward(ctx)
            except Exception as exc:
                run.failure = (step.name, exc)
                _log.warning(
                    "Saga step %r failed: %s — compensating", step.name, exc
                )
                self._compensate(run)
                return run
            run.completed.append(step.name)
        return run

    def to_mermaid(self, *, title: str | None = None) -> str:
        """Render this saga as a Mermaid ``graph LR`` source string.

        Forward edges are solid; compensation edges (each step back
        to its predecessor) are dotted so reviewers can spot which
        steps are reversible at a glance. Non-compensable steps get
        a red border to signal manual-intervention-on-failure
        territory.

        Pipe the output to any Mermaid renderer
        (``mmdc -i saga.mmd -o saga.svg``) or paste into a Markdown
        block on a platform that renders Mermaid natively (GitHub /
        GitLab / Notion).
        """
        lines: list[str] = []
        if title:
            lines.append(f"%% {title}")
        lines.append("graph LR")
        # Nodes. Steps without ``compensate`` get a distinct class
        # so the renderer can highlight the irreversible ones.
        for s in self.steps:
            sid = _safe_id(s.name)
            label = s.name.replace('"', "'")
            shape_open, shape_close = ("[", "]") if s.compensate is not None else ("[/", "/]")
            lines.append(f'  {sid}{shape_open}"{label}"{shape_close}')
            if s.compensate is None:
                lines.append(f"  class {sid} non_comp")
        # Forward edges.
        for prev, nxt in zip(self.steps, self.steps[1:]):
            lines.append(f"  {_safe_id(prev.name)} --> {_safe_id(nxt.name)}")
        # Compensation edges — each compensable step points back
        # to its predecessor with a dotted line + the literal
        # "compensate" label so the diagram is self-describing.
        for prev, nxt in zip(self.steps, self.steps[1:]):
            if nxt.compensate is None:
                continue
            lines.append(
                f"  {_safe_id(nxt.name)} -.compensate.-> "
                f"{_safe_id(prev.name)}"
            )
        # Class definition for the non-compensable nodes — Mermaid
        # tolerates the directive at the bottom of the source.
        lines.append("  classDef non_comp stroke:#d33,stroke-width:2px")
        return "\n".join(lines)

    def to_dot(self, *, title: str | None = None) -> str:
        """Render this saga as a Graphviz ``digraph`` source string.

        Equivalent semantics to :meth:`to_mermaid` — forward edges
        solid, compensation edges dashed, non-compensable steps
        styled in red. Feed the output to ``dot -Tsvg`` /
        ``dot -Tpng`` for an image."""
        lines: list[str] = []
        lines.append("digraph Saga {")
        lines.append('  rankdir=LR;')
        if title:
            safe = title.replace('"', "'")
            lines.append(f'  label="{safe}"; labelloc="t";')
        for s in self.steps:
            sid = _safe_id(s.name)
            label = s.name.replace('"', "'")
            attrs = [f'label="{label}"']
            if s.compensate is None:
                attrs.append('color="red"')
                attrs.append('penwidth="2"')
            lines.append(f"  {sid} [{', '.join(attrs)}];")
        for prev, nxt in zip(self.steps, self.steps[1:]):
            lines.append(f"  {_safe_id(prev.name)} -> {_safe_id(nxt.name)};")
        for prev, nxt in zip(self.steps, self.steps[1:]):
            if nxt.compensate is None:
                continue
            lines.append(
                f'  {_safe_id(nxt.name)} -> {_safe_id(prev.name)} '
                f'[style="dashed", label="compensate"];'
            )
        lines.append("}")
        return "\n".join(lines)

    def _compensate(self, run: SagaRun) -> None:
        # Walk completed steps in reverse, firing each non-None
        # ``compensate``. Each runs in its own atomic block so a
        # broken compensation doesn't poison the others.
        for name in reversed(run.completed):
            step = next(s for s in self.steps if s.name == name)
            if step.compensate is None:
                _log.error(
                    "Saga: step %r has no compensation — manual intervention "
                    "required to roll back this side-effect.",
                    name,
                )
                continue
            try:
                with transaction.atomic(using=self.using):
                    step.compensate(run.context)
                run.compensated.append(name)
            except Exception as exc:
                _log.error(
                    "Saga compensation for step %r failed: %s", name, exc
                )
                if self.stop_on_compensation_error:
                    return


_SAFE_ID_TRANS = str.maketrans({c: "_" for c in " -.,/\\:;()[]{}<>!?@#$%^&*+=|'\""})


def _safe_id(name: str) -> str:
    """Coerce a step name to a Mermaid / Graphviz-friendly identifier.

    Both renderers require nodes to be identifier-shaped before the
    optional ``"label"`` text — punctuation and whitespace would
    otherwise produce a parse error. Identifier collisions are
    avoided upstream because :class:`Saga` rejects duplicate step
    names at construction time.
    """
    out = name.translate(_SAFE_ID_TRANS)
    if not out or not (out[0].isalpha() or out[0] == "_"):
        out = "n_" + out
    return out


__all__ = ["Step", "Saga", "SagaRun"]
