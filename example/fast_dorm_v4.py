"""FastAPI example showcasing djanorm 4.0 production primitives.

Covers, in one ~150-line file:

- AsyncModel — sync ORM access raises AsyncOnlyError
- TenantModel + current_tenant() middleware (row-level tenancy)
- Query budget (HTTP SLA timeout) per request
- Streaming JSONL export (memory-bounded)
- N+1 detector as a logging middleware
- Idempotency keys on a non-idempotent POST
- LISTEN/NOTIFY → WebSocket fan-out
- Lifespan: auto-migrate at boot

Run with:
    uvicorn example.fast_dorm_v4:app --reload

Requires:
    pip install 'djanorm[postgresql,pydantic]' fastapi uvicorn
"""

from __future__ import annotations

import json
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Header, Request, WebSocket
from fastapi.responses import JSONResponse, StreamingResponse

import dorm
from dorm.contrib.asyncmodel import AsyncModel
from dorm.contrib.idempotency import IdempotencyRecord, idempotency_key
from dorm.contrib.listen_notify import anotify, listen
from dorm.contrib.nplusone import detect as nplus_one_detect
from dorm.contrib.streaming import astream_jsonl
from dorm.contrib.tenants_row import (
    TenantModel, current_tenant, make_async_tenant_manager,
)

log = logging.getLogger("fast_dorm_v4")


# ── Models ────────────────────────────────────────────────────────────────────


class Order(TenantModel, AsyncModel):
    """Tenant-scoped, async-only Order. Sync access raises
    `AsyncOnlyError`; reads/writes auto-filter on the active
    tenant_id pinned via the middleware below.

    The combined manager (``make_async_tenant_manager()``) ensures
    that the AsyncOnly enforcement isn't dropped by MRO order — the
    naive ``class Foo(TenantModel, AsyncModel)`` picks
    ``TenantManager`` and silently loses the async-only guard.
    """

    title = dorm.CharField(max_length=200)
    amount = dorm.IntegerField()
    status = dorm.CharField(max_length=20, default="pending")
    created_at = dorm.DateTimeField(auto_now_add=True)

    objects = make_async_tenant_manager()()

    class Meta:
        db_table = "orders"
        app_label = "fast_dorm_v4"


class Idempotency(IdempotencyRecord):
    class Meta:
        db_table = "idempotency_entries"
        app_label = "fast_dorm_v4"


# ── Lifespan: configure dorm + run migrations ─────────────────────────────────


@asynccontextmanager
async def lifespan(app: FastAPI):
    dorm.configure(
        DATABASES={
            "default": {
                "ENGINE": "postgresql",
                "NAME": "fast_dorm_v4",
                "USER": "postgres",
                "PASSWORD": "postgres",
                "HOST": "localhost",
                "PORT": 5432,
            }
        },
        INSTALLED_APPS=["fast_dorm_v4"],
    )
    # Auto-migrate at boot for serverless / ECS / Lambda. Skip in
    # production where you control the migration schedule.
    # cmd_migrate(<args>)
    yield


app = FastAPI(lifespan=lifespan)


# ── Middleware ────────────────────────────────────────────────────────────────


@app.middleware("http")
async def tenant_middleware(request: Request, call_next):
    """Pins ``current_tenant`` from the X-Tenant-ID header so every
    Order query auto-scopes."""
    tenant = request.headers.get("X-Tenant-ID")
    if tenant is None:
        return JSONResponse({"detail": "missing X-Tenant-ID"}, status_code=400)
    with current_tenant(tenant):
        return await call_next(request)


@app.middleware("http")
async def nplus_one_middleware(request: Request, call_next):
    """Logs N+1 detected per request without breaking the response."""
    with nplus_one_detect(raise_on_detect=False) as d:
        response = await call_next(request)
    if d.findings:
        log.warning("N+1 on %s: %s", request.url.path, d.report())
    return response


# ── Endpoints ─────────────────────────────────────────────────────────────────


@app.post("/orders")
async def create_order(
    body: dict,
    idempotency_key_header: str = Header(alias="Idempotency-Key"),
):
    """Idempotent create — same key → cached response."""
    with idempotency_key(idempotency_key_header, model=Idempotency) as ctx:
        if ctx.replay:
            return JSONResponse(
                ctx.cached_response,
                status_code=ctx.cached_status_code or 200,
            )
        order = await Order.objects.acreate(
            title=body["title"], amount=int(body["amount"])
        )
        # Notify WebSocket subscribers (per-tenant channel).
        await anotify(f"orders:{order.tenant_id}", json.dumps({"id": order.pk}))
        result = {"id": order.pk, "title": order.title, "amount": order.amount}
        ctx.store(result, status_code=201)
        return JSONResponse(result, status_code=201)


@app.get("/orders")
async def list_orders():
    """Per-request query budget — caps wall-clock and rows."""
    async with dorm.abudget(timeout_ms=200, max_rows=10_000):
        rows = [{"id": o.pk, "title": o.title, "amount": o.amount}
                async for o in Order.objects.afilter(status="pending")]
    return {"orders": rows}


@app.get("/orders/export.jsonl")
async def export_orders():
    """Streaming JSONL export — memory-bounded for million-row tables."""
    qs = Order.objects.afilter(status="completed")
    return StreamingResponse(
        astream_jsonl(qs, chunk_size=1000),
        media_type="application/x-ndjson",
        headers={"Content-Disposition": 'attachment; filename="orders.jsonl"'},
    )


@app.websocket("/orders/feed")
async def orders_feed(ws: WebSocket):
    """Real-time order feed via PG LISTEN/NOTIFY."""
    await ws.accept()
    tenant = ws.query_params.get("tenant")
    if not tenant:
        await ws.close(code=1008, reason="missing tenant")
        return
    async with listen(f"orders:{tenant}") as channel:
        async for n in channel:
            await ws.send_text(n.payload)
