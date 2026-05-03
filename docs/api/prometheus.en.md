# `dorm.contrib.prometheus`

Stdlib-only Prometheus text-exposition exporter — no
`prometheus_client` dependency. Connects to `post_query` to emit:

| Metric                                | Type      | Labels      |
|---------------------------------------|-----------|-------------|
| `dorm_queries_total`                  | counter   | `vendor`, `outcome` |
| `dorm_query_duration_seconds`         | histogram | `vendor`    |
| `dorm_pool_size`                      | gauge     | `alias`     |
| `dorm_pool_in_use`                    | gauge     | `alias`     |
| `dorm_cache_hits_total`               | counter   | `alias`     |
| `dorm_cache_misses_total`             | counter   | `alias`     |

## Quick start (FastAPI / any ASGI)

```python
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from dorm.contrib.prometheus import install, metrics_response

app = FastAPI()

@app.on_event("startup")
def startup():
    install()  # connect counters / histograms to dorm signals

@app.get("/metrics")
def metrics():
    return PlainTextResponse(
        metrics_response(),
        media_type="text/plain; version=0.0.4",
    )
```

## API

::: dorm.contrib.prometheus.install
::: dorm.contrib.prometheus.uninstall
::: dorm.contrib.prometheus.metrics_response
::: dorm.contrib.prometheus.record_cache_hit
::: dorm.contrib.prometheus.record_cache_miss

## Histogram buckets

Fixed layout (1 ms → 5 s, doubling):
``0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0``.
Apps that need richer / configurable buckets should swap to
`prometheus_client` and translate from the same dorm signals.
