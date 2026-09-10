# Copyright 2026 Query Farm LLC - https://query.farm
"""Cross-language Filter Encoding v2 decoder harness."""

from __future__ import annotations

import json
import sys
from typing import Any

import pyarrow as pa
from vgi.filter_v2 import (
    BooleanExpression,
    ColumnRef,
    Comparison,
    ExternalSet,
    FieldRef,
    In,
    IsNull,
    Literal,
    deserialize_snapshot,
)


def read_batch(path: str) -> pa.RecordBatch:
    with pa.ipc.open_stream(pa.memory_map(path, "rb")) as reader:
        return reader.read_next_batch()


def dump_expression(expression: Any) -> dict[str, Any]:
    if isinstance(expression, ColumnRef):
        return {"node": "column_ref", "column_name": expression.column_name,
                "column_index": expression.column_index}
    if isinstance(expression, FieldRef):
        return {"node": "field_ref", "expression": dump_expression(expression.expression),
                "field_name": expression.field_name, "field_index": expression.field_index}
    if isinstance(expression, Literal):
        return {"node": "literal", "value": expression.value.as_py(),
                "value_type": str(expression.value.type)}
    if isinstance(expression, Comparison):
        return {"node": "comparison", "op": expression.op.value,
                "left": dump_expression(expression.left), "right": dump_expression(expression.right)}
    if isinstance(expression, BooleanExpression):
        return {"node": expression.node, "children": [dump_expression(c) for c in expression.children]}
    if isinstance(expression, IsNull):
        return {"node": "is_null", "negated": expression.negated,
                "expression": dump_expression(expression.expression)}
    if isinstance(expression, In):
        values = expression.set.values.to_pylist()
        return {"node": "in", "negated": expression.negated,
                "expression": dump_expression(expression.expression), "values": values,
                "external": isinstance(expression.set, ExternalSet)}
    return {"node": type(expression).__name__}


def main(argv: list[str]) -> int:
    join_keys = [read_batch(path) for path in argv[2:]]
    output_schema = pa.schema([
        pa.field("n", pa.int64()),
        pa.field("name", pa.string()),
        pa.field("score", pa.float64()),
        pa.field("addr", pa.struct([pa.field("zip", pa.int64()), pa.field("city", pa.string())])),
        pa.field("key", pa.int64()),
    ])
    state = deserialize_snapshot(read_batch(argv[1]), output_schema=output_schema,
                                 join_keys=join_keys or None)
    json.dump({"semantics": state.semantics,
               "predicates": [{"id": p.id, "mode": p.mode.value, "source": p.source.value,
                               "expression": dump_expression(p.expression)}
                              for p in state.predicates]}, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
