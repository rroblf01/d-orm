# Contribs de observabilidad

`dorm.contrib.querylog`, `dorm.contrib.querycount` y los helpers
de test `assertNumQueries` escuchan las señales `pre_query` /
`post_query` via la primitiva compartida `ScopedCollector` —
aislamiento per-task via `ContextVar`, así requests concurrentes
no se mezclan contadores.

## `dorm.contrib.querylog`

Captura cada sentencia SQL dentro de un context manager. Útil para
inspeccionar el mix de queries por request, asertar en tests que
un code path no regresiona en count / duration, y surface patrones
N+1 agrupando por template SQL.

```python
from dorm.contrib.querylog import QueryLog, QueryLogASGIMiddleware

with QueryLog() as log:
    do_work()

for stats in log.summary():
    print(stats.template, stats.count, stats.p95_ms)

# Middleware ASGI — log aterriza en scope["dorm_querylog"]
app = QueryLogASGIMiddleware(your_asgi_app)
```


## `dorm.contrib.querycount`

Guard ligero N+1 alrededor de un bloque de código. Cuenta queries
dentro del bloque via `pre_query` y emite un único `WARNING` si
el count cruza un umbral.

```python
from dorm.contrib.querycount import query_count_guard

with query_count_guard(warn_above=20, label="GET /articles"):
    return [article_dict(a) for a in Article.objects.all()]
```

`warn_above` cae a `settings.QUERY_COUNT_WARN` si no se pasa;
`None` (default) deja el guard inerte (cuenta sin avisar).


## `dorm.test.assertNumQueries` / `assertMaxQueries`

Helpers de test paridad Django. Forma context-manager asserta
count exact / ≤-N al salir; forma decorator envuelve función
test sync o `async def`. Scopes anidados acumulan — un guard
exterior cuenta queries lanzadas dentro de inner anidado
(matches Django).

```python
from dorm.test import assertNumQueries, assertMaxQueriesFactory

def test_list_view(transactional_db):
    with assertNumQueries(3):
        list(Article.objects.select_related("author")[:10])

@assertMaxQueriesFactory(5)
def test_dashboard(transactional_db):
    render_dashboard()
```

