"""PostgreSQL full-text search helpers.

Wraps the ``to_tsvector`` / ``to_tsquery`` / ``ts_rank`` family so
applications can build search queries with the same idiom as the rest
of dorm — annotate, filter, order — without dropping to raw SQL::

    from dorm import F, Value
    from dorm.search import SearchVector, SearchQuery, SearchRank

    # Filter rows matching a query
    Article.objects.filter(title__search="postgres")

    # Annotate a relevance score and sort by it
    qs = (
        Article.objects
            .annotate(
                rank=SearchRank(
                    SearchVector("title", "body"),
                    SearchQuery("postgres"),
                )
            )
            .filter(rank__gt=0)
            .order_by("-rank")
    )

The features are PostgreSQL-only — emitting them against SQLite
raises :class:`NotImplementedError` at compile time. For SQLite reach
for the ``LIKE`` / FTS5 virtual-table approach instead.
"""
from __future__ import annotations

from typing import Any

from .conf import _validate_identifier
from .exceptions import ImproperlyConfigured


# Subset of PostgreSQL text-search configurations safe to splice into
# SQL without quoting. Matches ``CREATE TEXT SEARCH CONFIGURATION``
# names — alphanumerics + underscore only.
import re as _re

_SAFE_CONFIG = _re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

_CONFIG_RE = _re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _validate_config(value: str) -> str:
    if not isinstance(value, str) or not _CONFIG_RE.match(value):
        raise ImproperlyConfigured(
            f"Search config {value!r} must be a SQL identifier "
            "(letters / digits / underscore, leading non-digit)."
        )
    return value


class SearchVector:
    """Wrap one or more columns as a ``to_tsvector(config, col || ' ' || ...)``
    expression. Use as the first argument to :class:`SearchQuery`'s
    match operator (or :class:`SearchRank`).

    Args:
        *fields: column names to combine into the vector.
        config: PostgreSQL search configuration. Defaults to
            ``"english"`` — change to ``"spanish"`` etc. as needed.
        weight: optional ``"A"`` / ``"B"`` / ``"C"`` / ``"D"`` weight
            label; rank functions multiply matches in a weighted column
            by 1.0 / 0.4 / 0.2 / 0.1 respectively.
    """

    def __init__(
        self,
        *fields: str,
        config: str = "english",
        weight: str | None = None,
    ) -> None:
        if not fields:
            raise ImproperlyConfigured("SearchVector requires at least one field.")
        for f in fields:
            _validate_identifier(f, kind="SearchVector field")
        self.fields = list(fields)
        self.config = _validate_config(config)
        if weight is not None and weight not in {"A", "B", "C", "D"}:
            raise ImproperlyConfigured(
                "SearchVector(weight=...) must be 'A', 'B', 'C' or 'D'."
            )
        self.weight = weight

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        ta = f'"{table_alias}".' if table_alias else ""
        cols = " || ' ' || ".join(
            f"COALESCE({ta}\"{f}\"::text, '')" for f in self.fields
        )
        sql = f"to_tsvector('{self.config}', {cols})"
        if self.weight:
            sql = f"setweight({sql}, '{self.weight}')"
        return sql, []

    def __repr__(self) -> str:
        return (
            f"SearchVector({', '.join(repr(f) for f in self.fields)}, "
            f"config={self.config!r}, weight={self.weight!r})"
        )


class SearchQuery:
    """Wrap a search string in ``plainto_tsquery``, ``websearch_to_tsquery``
    or ``to_tsquery`` depending on *search_type*.

    Args:
        value: the search string.
        config: search configuration (default ``"english"``).
        search_type: one of:

            - ``"plain"`` (default) — words are AND-ed; punctuation is
              stripped.
            - ``"websearch"`` — accepts ``"quoted phrase"``, ``OR``,
              ``-exclude`` syntax. PostgreSQL ≥ 11.
            - ``"raw"`` — passes through verbatim to ``to_tsquery``.
              The caller takes responsibility for the syntax.
    """

    _TYPES = {
        "plain": "plainto_tsquery",
        "websearch": "websearch_to_tsquery",
        "raw": "to_tsquery",
    }

    def __init__(
        self,
        value: str,
        *,
        config: str = "english",
        search_type: str = "plain",
        invert: bool = False,
    ) -> None:
        if search_type not in self._TYPES:
            raise ImproperlyConfigured(
                f"SearchQuery(search_type=...) must be one of {sorted(self._TYPES)}."
            )
        self.value = value
        self.config = _validate_config(config)
        self.search_type = search_type
        self.invert = invert

    def __invert__(self) -> "SearchQuery":
        return SearchQuery(
            self.value,
            config=self.config,
            search_type=self.search_type,
            invert=not self.invert,
        )

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        fn = self._TYPES[self.search_type]
        prefix = "!!" if self.invert else ""
        return f"{prefix}{fn}('{self.config}', %s)", [self.value]

    def __repr__(self) -> str:
        prefix = "~" if self.invert else ""
        return (
            f"{prefix}SearchQuery({self.value!r}, "
            f"config={self.config!r}, search_type={self.search_type!r})"
        )


class SearchRank:
    """Compute ``ts_rank(vector, query)`` — float relevance score.

    ``cover_density=True`` switches to ``ts_rank_cd`` (cover density),
    which weights matches by proximity and document length differently.
    Use whichever ranks your queries best — the difference is small in
    practice.
    """

    def __init__(
        self,
        vector: SearchVector,
        query: SearchQuery,
        *,
        cover_density: bool = False,
    ) -> None:
        if not isinstance(vector, SearchVector):
            raise ImproperlyConfigured(
                "SearchRank(vector=...) must be a SearchVector instance."
            )
        if not isinstance(query, SearchQuery):
            raise ImproperlyConfigured(
                "SearchRank(query=...) must be a SearchQuery instance."
            )
        self.vector = vector
        self.query = query
        self.cover_density = cover_density

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        v_sql, v_params = self.vector.as_sql(table_alias)
        q_sql, q_params = self.query.as_sql(table_alias)
        fn = "ts_rank_cd" if self.cover_density else "ts_rank"
        return f"{fn}({v_sql}, {q_sql})", v_params + q_params

    def __repr__(self) -> str:
        return f"SearchRank({self.vector!r}, {self.query!r})"


class SearchHeadline:
    """Compute ``ts_headline(<config>, document, query [, options])``
    — render the matched fragment with the matching tokens wrapped
    in ``<b>...</b>`` (or whatever ``StartSel`` / ``StopSel`` you
    configure). Useful for search-result snippets.

    *expression* is the column / expression containing the document
    text (typically a string ``F("body")`` or a string literal).

    *query* is a :class:`SearchQuery` instance reused from the same
    search call.

    *options* is a free-form ``dict`` mapping the standard
    ``ts_headline`` keys to values — ``MaxWords``, ``MinWords``,
    ``ShortWord``, ``HighlightAll``, ``MaxFragments``,
    ``StartSel``, ``StopSel``, ``FragmentDelimiter``. Values are
    passed as bound parameters; PG joins them in the
    ``<key>=<value>`` syntax via ``%s`` placeholders.
    """

    def __init__(
        self,
        expression: Any,
        query: "SearchQuery",
        *,
        config: str = "english",
        options: dict[str, Any] | None = None,
    ) -> None:
        if not isinstance(query, SearchQuery):
            raise ImproperlyConfigured(
                "SearchHeadline(query=...) must be a SearchQuery instance."
            )
        if not _SAFE_CONFIG.match(config):
            raise ImproperlyConfigured(
                f"SearchHeadline(config={config!r}) — config must be a "
                "PostgreSQL regconfig identifier."
            )
        self.expression = expression
        self.query = query
        self.config = config
        self.options = options or {}

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        from .functions import _compile_expr

        expr_sql, expr_params = _compile_expr(self.expression, table_alias)
        q_sql, q_params = self.query.as_sql(table_alias)
        params: list = list(expr_params) + list(q_params)
        if self.options:
            for k in self.options:
                if not _SAFE_CONFIG.match(k):
                    raise ImproperlyConfigured(
                        f"SearchHeadline option {k!r} is not a valid identifier."
                    )
            # Inline the options dict as a string literal — PG's
            # ``ts_headline(... , 'opt=val')`` syntax doesn't accept
            # bound parameters for the options string. Single-quote
            # values are SQL-escaped so a literal ``'`` in
            # ``StartSel`` etc. survives.
            options_str = ", ".join(
                f"{k}={str(v).replace(chr(39), chr(39) + chr(39))}"
                for k, v in self.options.items()
            )
            return (
                f"ts_headline('{self.config}', {expr_sql}, {q_sql}, "
                f"'{options_str}')",
                params,
            )
        return (
            f"ts_headline('{self.config}', {expr_sql}, {q_sql})",
            params,
        )

    def __repr__(self) -> str:
        return f"SearchHeadline({self.expression!r}, {self.query!r})"


class TrigramSimilarity:
    """Compile to ``similarity(<expression>, %s)`` — PostgreSQL
    ``pg_trgm`` extension. Used in ``annotate(score=TrigramSimilarity(
    'name', 'foo'))`` followed by ``order_by('-score')`` for fuzzy
    search ranking.

    Requires the extension::

        CREATE EXTENSION IF NOT EXISTS pg_trgm;

    Pair with a GIN/GIST index for performance::

        Index(fields=["name"], opclass="gin_trgm_ops", method="GIN")
    """

    def __init__(self, expression: Any, value: str) -> None:
        if not isinstance(value, str):
            raise ImproperlyConfigured(
                f"TrigramSimilarity value must be a string; got "
                f"{type(value).__name__}."
            )
        self.expression = expression
        self.value = value

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        from .expressions import F
        from .functions import _compile_expr

        expr = self.expression
        # Bare strings are treated as column references (the common
        # case: ``TrigramSimilarity("name", "alice")``); explicit F /
        # Value wrappers are honoured if the caller passed them.
        if isinstance(expr, str):
            expr = F(expr)
        expr_sql, expr_params = _compile_expr(expr, table_alias)
        return f"similarity({expr_sql}, %s)", list(expr_params) + [self.value]

    def __repr__(self) -> str:
        return f"TrigramSimilarity({self.expression!r}, {self.value!r})"


class TrigramWordSimilarity:
    """Compile to ``word_similarity(%s, <expression>)`` — PG ``pg_trgm``
    extension's word-aware variant. Better when the search target has
    multiple words and the user query is a single word."""

    def __init__(self, value: str, expression: Any) -> None:
        if not isinstance(value, str):
            raise ImproperlyConfigured(
                f"TrigramWordSimilarity value must be a string; got "
                f"{type(value).__name__}."
            )
        self.value = value
        self.expression = expression

    def as_sql(self, table_alias: str | None = None, **kwargs: Any) -> tuple[str, list]:
        from .expressions import F
        from .functions import _compile_expr

        expr = self.expression
        if isinstance(expr, str):
            expr = F(expr)
        expr_sql, expr_params = _compile_expr(expr, table_alias)
        return f"word_similarity(%s, {expr_sql})", [self.value] + list(expr_params)

    def __repr__(self) -> str:
        return f"TrigramWordSimilarity({self.value!r}, {self.expression!r})"


def search_index(
    table: str, *fields: str, name: str | None = None, config: str = "english"
) -> str:
    """Render a ``CREATE INDEX`` statement for a functional GIN
    index over ``to_tsvector(config, col || ' ' || …)``. Returns
    the raw SQL string suitable for a migration's
    :class:`~dorm.migrations.operations.RunSQL` step::

        from dorm.migrations.operations import RunSQL
        from dorm.search import search_index

        operations = [
            RunSQL(
                search_index("articles", "title", "body"),
                reverse_sql='DROP INDEX IF EXISTS ix_articles_search'
            ),
        ]

    The functional-index path goes through ``RunSQL`` rather than
    :class:`~dorm.indexes.Index` because the ``to_tsvector`` shape
    falls outside the strict allowlist on
    :class:`~dorm.indexes.Index` (which intentionally accepts only
    ``FN(col1, col2)``-shaped expressions to keep migration
    autodetection sound).

    PostgreSQL-only. Default *config* is ``english``; override per-
    locale.
    """
    if not fields:
        raise ImproperlyConfigured("search_index requires at least one field")
    _validate_identifier(table, kind="search_index table")
    for f in fields:
        _validate_identifier(f, kind="search_index field")
    config = _validate_config(config)

    idx_name = name or f"ix_{table}_search"
    _validate_identifier(idx_name, kind="search_index name")

    expr = " || ' ' || ".join(f"coalesce({f}::text, '')" for f in fields)
    return (
        f'CREATE INDEX IF NOT EXISTS "{idx_name}" '
        f'ON "{table}" USING GIN '
        f"(to_tsvector('{config}', ({expr})))"
    )


__all__ = [
    "SearchVector",
    "SearchQuery",
    "SearchRank",
    "SearchHeadline",
    "TrigramSimilarity",
    "TrigramWordSimilarity",
    "search_index",
]
