# `dorm.contrib.prometheus`

Exporter de exposición text-format de Prometheus solo-stdlib — sin
dependencia de `prometheus_client`. Engancha `post_query` para emitir:

| Métrica                               | Tipo      | Labels      |
|---------------------------------------|-----------|-------------|
| `dorm_queries_total`                  | counter   | `vendor`, `outcome` |
| `dorm_query_duration_seconds`         | histogram | `vendor`    |
| `dorm_pool_size`                      | gauge     | `alias`     |
| `dorm_pool_in_use`                    | gauge     | `alias`     |
| `dorm_cache_hits_total`               | counter   | `alias`     |
| `dorm_cache_misses_total`             | counter   | `alias`     |

## Quick start (FastAPI / cualquier ASGI)

```python
from fastapi import FastAPI
from fastapi.responses import PlainTextResponse
from dorm.contrib.prometheus import install, metrics_response

app = FastAPI()

@app.on_event("startup")
def startup():
    install()  # conecta contadores / histograms a las señales dorm

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

## Buckets del histograma

Layout fijo (1 ms → 5 s, doblando):
``0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0``.
Apps que necesiten buckets más ricos / configurables deberían
pasar a `prometheus_client` y traducir desde las mismas señales dorm.
