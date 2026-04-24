"""Index definitions for dorm models."""
from __future__ import annotations


class Index:
    """Represents a database index for use in Meta.indexes."""

    def __init__(
        self,
        fields: list[str],
        name: str | None = None,
        unique: bool = False,
    ) -> None:
        self.fields = list(fields)
        self.unique = unique
        self._name = name

    def get_name(self, model_name: str) -> str:
        if self._name:
            return self._name
        suffix = "_".join(self.fields)
        prefix = "uniq" if self.unique else "idx"
        return f"{prefix}_{model_name.lower()}_{suffix}"

    @property
    def name(self) -> str:
        return self._name or ""

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Index):
            return NotImplemented
        return self.fields == other.fields and self.unique == other.unique and self._name == other._name

    def __repr__(self) -> str:
        return f"Index(fields={self.fields!r}, unique={self.unique!r}, name={self._name!r})"
