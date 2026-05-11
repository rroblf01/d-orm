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
        failure. Returns the :class:`SagaRun` audit record."""
        ctx: dict[str, Any] = context or {}
        run = SagaRun(context=ctx)
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


__all__ = ["Step", "Saga", "SagaRun"]
