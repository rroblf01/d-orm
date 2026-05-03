# Observability contribs

`dorm.contrib.querylog`, `dorm.contrib.querycount` and the
`assertNumQueries` test helpers all listen on the `pre_query` /
`post_query` signals via the shared `ScopedCollector` primitive —
per-task isolation via `ContextVar`, so concurrent requests don't
bleed into each other's counters.

## `dorm.contrib.querylog`

Captures every SQL statement inside a context-manager block.
Useful for inspecting the query mix per request, asserting in
tests that a code path doesn't regress on count / duration, and
surfacing N+1 patterns by grouping captured statements by SQL
template.

```python
from dorm.contrib.querylog import QueryLog, QueryLogASGIMiddleware

with QueryLog() as log:
    do_work()

for stats in log.summary():
    print(stats.template, stats.count, stats.p95_ms)

# ASGI middleware — log lands on scope["dorm_querylog"]
app = QueryLogASGIMiddleware(your_asgi_app)
```

::: dorm.contrib.querylog.QueryLog
::: dorm.contrib.querylog.QueryRecord
::: dorm.contrib.querylog.TemplateStats
::: dorm.contrib.querylog.QueryLogASGIMiddleware
::: dorm.contrib.querylog.query_log

## `dorm.contrib.querycount`

Lightweight N+1 guard around a code block. Counts queries inside
the block via `pre_query` and emits a single `WARNING` if the count
crosses a threshold.

```python
from dorm.contrib.querycount import query_count_guard

with query_count_guard(warn_above=20, label="GET /articles"):
    return [article_dict(a) for a in Article.objects.all()]
```

`warn_above` falls back to `settings.QUERY_COUNT_WARN` when not
given; `None` (the default) leaves the guard inert (counts but
never warns).

::: dorm.contrib.querycount.query_count_guard
::: dorm.contrib.querycount.QueryCount

## `dorm.test.assertNumQueries` / `assertMaxQueries`

Django-parity test helpers. Context-manager forms assert exact /
≤-N query counts on exit; decorator forms wrap a sync or `async
def` test function. Nested scopes accumulate — an outer guard
counts queries fired inside any nested inner one (matches Django's
behaviour).

```python
from dorm.test import assertNumQueries, assertMaxQueriesFactory

def test_list_view(transactional_db):
    with assertNumQueries(3):
        list(Article.objects.select_related("author")[:10])

@assertMaxQueriesFactory(5)
def test_dashboard(transactional_db):
    render_dashboard()
```

::: dorm.test.assertNumQueries
::: dorm.test.assertMaxQueries
::: dorm.test.assertNumQueriesFactory
::: dorm.test.assertMaxQueriesFactory
