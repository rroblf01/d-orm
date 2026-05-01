"""``VectorField`` — vector column type for pgvector / sqlite-vec.

Backend-aware:

* **PostgreSQL** + pgvector — column type ``vector(N)``, value bound
  as the text form ``"[v1,v2,…]"`` (pgvector accepts that without
  the bundled adapter; with the ``pgvector`` package installed,
  ``numpy.ndarray`` binds natively too).
* **SQLite** + sqlite-vec — column type ``BLOB``, value packed as
  little-endian float32 via :mod:`struct`. sqlite-vec's
  ``vec_distance_*`` functions accept either BLOB or JSON text;
  BLOB is faster and what the upstream docs recommend for stored
  vectors.

Same ``VectorField(dimensions=N)`` declaration works on both
backends — the SQL type is decided at migrate time by
:meth:`db_type`, and the value adapter is decided at write time by
the connection's ``vendor`` attribute.
"""

from __future__ import annotations

import struct
from typing import Any, Sequence

from ...exceptions import ValidationError
from ...fields import Field


def _pack_float32(seq: Sequence[float]) -> bytes:
    """Serialise *seq* to packed little-endian float32, the wire
    format sqlite-vec expects for BLOB-typed vector inputs."""
    return struct.pack(f"<{len(seq)}f", *(float(x) for x in seq))


def _unpack_float32(data: bytes) -> list[float]:
    if len(data) % 4 != 0:
        raise ValidationError(
            f"VectorField BLOB length {len(data)} not a multiple of 4 "
            "bytes — column corrupted or written by something other "
            "than dorm's float32 adapter."
        )
    n = len(data) // 4
    return list(struct.unpack(f"<{n}f", data))


class VectorField(Field[list]):
    """Column storing a fixed-length float vector.

    Args:
        dimensions: required vector length. The column is declared
            ``vector(dimensions)`` on PostgreSQL and ``BLOB`` on
            SQLite (the size is enforced in Python on both backends
            because SQLite's BLOB has no length constraint).

    The Python type is ``list[float]`` on read; on write we accept
    ``list`` / ``tuple`` / ``numpy.ndarray`` / pgvector's own
    ``Vector`` — anything iterable that yields numeric values. The
    value goes out as the right wire format for the active backend:

    * pgvector: ``"[v1,v2,…]"`` text form.
    * sqlite-vec: packed little-endian float32 BLOB.
    """

    def __init__(self, dimensions: int, **kwargs: Any) -> None:
        if not isinstance(dimensions, int) or dimensions <= 0:
            raise ValueError(
                "VectorField(dimensions=…) must be a positive int; "
                f"got {dimensions!r}."
            )
        # pgvector caps dimensions at 16000 for the dense ``vector``
        # type. sqlite-vec doesn't impose a hard limit but the same
        # 16000 ceiling is a sane sanity-check across backends.
        if dimensions > 16000:
            raise ValueError(
                "VectorField caps dimensions at 16000 (pgvector's "
                f"limit). Got {dimensions}. For higher-dim use "
                "pgvector ≥ 0.7's ``halfvec`` / ``sparsevec`` (not "
                "yet wrapped by djanorm)."
            )
        self.dimensions = dimensions
        super().__init__(**kwargs)

    def db_type(self, connection: Any) -> str | None:
        vendor = getattr(connection, "vendor", "sqlite")
        if vendor == "postgresql":
            return f"vector({self.dimensions})"
        if vendor == "libsql":
            # libsql ships native ``F32_BLOB(N)`` vector columns plus
            # the ``vector_distance_*`` family of functions — no
            # extension load required. The dimension count IS stored
            # in the column type, so a future ``vector_concat`` /
            # ``vector_extract`` operator can validate against it.
            return f"F32_BLOB({self.dimensions})"
        if vendor == "sqlite":
            # sqlite-vec stores vectors in BLOB columns and exposes
            # ``vec_distance_L2`` / ``vec_distance_cosine`` over them.
            # The dimension count is enforced in Python because
            # SQLite's BLOB carries no length metadata.
            return "BLOB"
        # Unknown backend — skip the column entirely so the rest of
        # the migration stays applicable.
        return None

    def to_python(self, value: Any) -> Any:
        if value is None:
            return None
        # SQLite path: BLOB → packed float32 bytes.
        if isinstance(value, (bytes, bytearray, memoryview)):
            data = bytes(value)
            return _unpack_float32(data)
        # pgvector text path: ``"[1,2,3]"``.
        if isinstance(value, str):
            stripped = value.strip()
            if stripped.startswith("[") and stripped.endswith("]"):
                stripped = stripped[1:-1]
            if not stripped:
                return []
            return [float(x) for x in stripped.split(",")]
        # numpy / iterable.
        if hasattr(value, "tolist"):
            return [float(x) for x in value.tolist()]
        if isinstance(value, (list, tuple)):
            return [float(x) for x in value]
        return [float(x) for x in value]

    def get_db_prep_value(self, value: Any) -> Any:
        """Adapt *value* to the wire format the active backend
        expects.

        Detects the backend by peeking at the model's default
        connection wrapper (``dorm.db.connection.get_connection()``).
        That works for the common case of a single ``DATABASES``
        alias; multi-database setups should make sure
        :class:`VectorField` is only used on tables routed to a
        consistent backend.

        Returns:
            * ``bytes`` for SQLite — packed little-endian float32,
                what sqlite-vec stores natively.
            * ``str`` for PostgreSQL — ``[v1,…]`` text form, what
                pgvector accepts even without the ``pgvector``
                Python package installed.
        """
        if value is None:
            return None
        seq = self._coerce_sequence(value)
        if len(seq) != self.dimensions:
            raise ValidationError(
                f"Field {self.name!r}: expected {self.dimensions}-d "
                f"vector, got {len(seq)} components."
            )
        # Lazy backend detection: the field doesn't know which
        # connection alias the row is headed for, so we read the
        # default. Multi-DB users with mixed-vendor aliases need to
        # subclass and override this — surfaced in the docs.
        from ...db.connection import get_connection

        try:
            vendor = getattr(get_connection(), "vendor", "postgresql")
        except Exception:
            # If the connection isn't configured yet (rare — happens
            # in unit tests that import the module before
            # ``configure``), fall back to the pgvector text form.
            vendor = "postgresql"
        if vendor in ("sqlite", "libsql"):
            # Both SQLite (sqlite-vec) and libsql (native F32_BLOB)
            # accept the same little-endian packed-float32 wire
            # format. libsql's ``vector32(?)`` / ``vector_distance_*``
            # SQL functions read it directly.
            return _pack_float32(seq)
        return "[" + ",".join(repr(float(x)) for x in seq) + "]"

    def from_db_value(self, value: Any) -> Any:
        result = self.to_python(value)
        # Mirror the write-side dimension check on read so a
        # corrupted column / cross-dim migration doesn't silently
        # round-trip the wrong shape — it surfaces as a
        # :class:`ValidationError` at hydration time.
        if result is not None and len(result) != self.dimensions:
            raise ValidationError(
                f"Field {self.name!r}: stored vector has {len(result)} "
                f"components, expected {self.dimensions}. Likely a "
                f"cross-dimension migration or corrupted column."
            )
        return result

    @staticmethod
    def _coerce_sequence(value: Any) -> Sequence[Any]:
        if isinstance(value, (bytes, bytearray, memoryview)):
            return _unpack_float32(bytes(value))
        if isinstance(value, str):
            stripped = value.strip().strip("[]")
            if not stripped:
                return []
            return [float(x) for x in stripped.split(",")]
        if hasattr(value, "tolist"):
            return value.tolist()
        if isinstance(value, (list, tuple)):
            return value
        return list(value)
