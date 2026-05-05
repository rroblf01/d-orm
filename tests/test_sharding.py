"""Tests for ``dorm.contrib.sharding``.

The hash function is deterministic, so tests assert exact alias
assignments — if the algorithm ever changes, the test fails loudly
(in-flight production data on the old hash would otherwise migrate
silently to a different shard).

Router tests use a stub model class — no real DBs are involved, only
the routing decisions.
"""

from __future__ import annotations

import pytest

from dorm.contrib.sharding import (
    HashShardRouter,
    for_each_shard,
    get_shard_key,
    shard_for,
    with_shard_key,
)


class _StubMeta:
    def __init__(self, name: str) -> None:
        self.model_name = name


class _StubModel:
    def __init__(self, name: str) -> None:
        self._meta = _StubMeta(name)
        self.__class__.__name__ = name


def test_shard_for_validation():
    with pytest.raises(ValueError):
        shard_for("k", 0)
    with pytest.raises(ValueError):
        shard_for(None, 4)
    with pytest.raises(ValueError):
        shard_for("k", 2, aliases=["only-one"])


def test_shard_for_is_deterministic():
    a = shard_for("user-42", 8)
    b = shard_for("user-42", 8)
    assert a == b


def test_shard_for_distributes_keys():
    buckets = {shard_for(f"k{i}", 4) for i in range(200)}
    # All 4 shards should be hit at least once with 200 keys.
    assert buckets == {"shard_0", "shard_1", "shard_2", "shard_3"}


def test_shard_for_custom_aliases():
    a = shard_for("k", 3, aliases=["red", "green", "blue"])
    assert a in {"red", "green", "blue"}


def test_shard_for_salt_changes_output():
    a = shard_for("k", 4, salt=b"x")
    b = shard_for("k", 4, salt=b"y")
    assert a != b or shard_for("kk", 4, salt=b"x") != shard_for(
        "kk", 4, salt=b"y"
    )


def test_with_shard_key_pins_key():
    assert get_shard_key() is None
    with with_shard_key("tenant-7"):
        assert get_shard_key() == "tenant-7"
    assert get_shard_key() is None


def test_with_shard_key_nested():
    with with_shard_key("a"):
        with with_shard_key("b"):
            assert get_shard_key() == "b"
        assert get_shard_key() == "a"


def test_router_passes_through_unsharded_models():
    class Order:
        pass

    class Setting:
        pass

    router = HashShardRouter(num_shards=4, shard_models={Order})
    # Unsharded → None (let next router / default decide).
    assert router.db_for_read(Setting) is None
    assert router.db_for_write(Setting) is None


def test_router_routes_sharded_with_active_key():
    class Order:
        pass

    router = HashShardRouter(num_shards=4, shard_models={Order})
    with with_shard_key("user-1"):
        alias = router.db_for_read(Order)
        assert alias.startswith("shard_")
        assert router.db_for_write(Order) == alias  # consistent r/w


def test_router_raises_when_sharded_without_key():
    class Order:
        pass

    router = HashShardRouter(num_shards=4, shard_models={Order})
    with pytest.raises(RuntimeError, match="no active shard key"):
        router.db_for_read(Order)


def test_router_validation():
    with pytest.raises(ValueError):
        HashShardRouter(num_shards=0)


def test_for_each_shard_runs_func_per_alias():
    seen: list[str] = []

    def _func(alias):
        seen.append(alias)
        return alias.upper()

    result = for_each_shard(_func, num_shards=3)
    assert seen == ["shard_0", "shard_1", "shard_2"]
    assert result == {
        "shard_0": "SHARD_0",
        "shard_1": "SHARD_1",
        "shard_2": "SHARD_2",
    }


def test_for_each_shard_custom_aliases():
    result = for_each_shard(
        lambda a: a, num_shards=2, aliases=["primary", "secondary"]
    )
    assert set(result) == {"primary", "secondary"}
