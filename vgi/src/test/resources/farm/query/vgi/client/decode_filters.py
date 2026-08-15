# Copyright 2026 Query Farm LLC - https://query.farm
"""Decode Java-encoded pushdown filters with vgi-python's reference decoder.

Cross-language conformance harness for ``PushdownFiltersEncoder``: the Java
side writes the filter batch (and any join-key batches) to files and runs this
script through vgi-python, which decodes them with the canonical
``deserialize_filters``. The parsed AST is printed as JSON on stdout so the
Java test can assert on it.

Usage:
    python decode_filters.py <filters.ipc> [<join_keys_0.ipc> ...]
"""

from __future__ import annotations

import json
import sys
from typing import Any

import pyarrow as pa
from vgi.table_filter_pushdown import (
    AndFilter,
    ConstantFilter,
    Filter,
    InFilter,
    IsNotNullFilter,
    IsNullFilter,
    OrFilter,
    StructFilter,
    deserialize_filters,
)


def read_batch(path: str) -> pa.RecordBatch:
    with pa.ipc.open_stream(pa.memory_map(path, "rb")) as reader:
        return reader.read_next_batch()


def dump(f: Filter) -> dict[str, Any]:
    base: dict[str, Any] = {
        "column_name": f.column_name,
        "column_index": f.column_index,
    }
    if isinstance(f, ConstantFilter):
        return base | {
            "type": "constant",
            "op": f.op.value,
            "value": f.value.as_py(),
            "value_type": str(f.value.type),
        }
    if isinstance(f, IsNullFilter):
        return base | {"type": "is_null"}
    if isinstance(f, IsNotNullFilter):
        return base | {"type": "is_not_null"}
    if isinstance(f, InFilter):
        return base | {
            "type": "in",
            "values": f.values.to_pylist(),
            "value_type": str(f.values.type),
        }
    if isinstance(f, AndFilter):
        return base | {"type": "and", "children": [dump(c) for c in f.children]}
    if isinstance(f, OrFilter):
        return base | {"type": "or", "children": [dump(c) for c in f.children]}
    if isinstance(f, StructFilter):
        return base | {
            "type": "struct",
            "child_index": f.child_index,
            "child_name": f.child_name,
            "child_filter": dump(f.child_filter),
        }
    return base | {"type": type(f).__name__}


def main(argv: list[str]) -> int:
    filters_path = argv[1]
    join_keys = [read_batch(p) for p in argv[2:]]
    parsed = deserialize_filters(read_batch(filters_path), join_keys or None)
    json.dump(
        {"version": parsed.version, "filters": [dump(f) for f in parsed.filters]},
        sys.stdout,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
