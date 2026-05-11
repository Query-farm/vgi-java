// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * {@code constant_columns(count BIGINT [const], *values [const, any])} —
 * generates {@code count} rows where each varargs value becomes one constant
 * column ({@code col_0}, {@code col_1}, ...). Mirrors vgi-go
 * {@code ConstantColumnsFunction}.
 */
public final class ConstantColumnsFunction implements TableFunction {

    @Override public String name() { return "constant_columns"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Generates rows with constant values from varargs");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.positional("count", 0, Schemas.INT64),
                new ArgSpec("values", 1, new ArrowType.Null(), "", true, false, "",
                        List.of(), /*varargs=*/true, /*anyType=*/true));
    }

    @Override public BindResponse onBind(TableBindParams p) {
        List<Object> positionals = p.arguments().positional();
        // positionals[0] is count, positionals[1..] are the varargs values.
        List<Field> fields = new ArrayList<>();
        for (int i = 1; i < positionals.size(); i++) {
            ArrowType wireType = p.arguments().positionalTypeAt(i);
            fields.add(buildField("col_" + (i - 1), positionals.get(i), wireType));
        }
        if (fields.isEmpty()) {
            // Catalog enumeration with no varargs supplied — placeholder.
            fields.add(Schemas.nullable("placeholder", Schemas.INT64));
        }
        return BindResponse.forSchema(farm.query.vgi.internal.SchemaUtil.serializeSchema(
                new Schema(fields)));
    }

    /** Build a Field for a varargs value. Uses the wire-derived ArrowType
     *  when available (preserves TINYINT vs BIGINT, FLOAT vs DOUBLE, etc.);
     *  falls back to inference from the Java runtime class otherwise. */
    private static Field buildField(String name, Object v, ArrowType wireType) {
        if (v instanceof java.util.Map<?, ?> m) {
            List<Field> children = new java.util.ArrayList<>();
            for (java.util.Map.Entry<?, ?> e : m.entrySet()) {
                children.add(buildField(e.getKey().toString(), e.getValue(), null));
            }
            return new Field(name, new FieldType(true, new ArrowType.Struct(), null), children);
        }
        if (wireType != null && !(wireType instanceof ArrowType.List) && !(wireType instanceof ArrowType.Struct)) {
            return new Field(name, new FieldType(true, wireType, null), null);
        }
        return new Field(name, new FieldType(true, listOrScalarType(v), null), listChildren(v, "item"));
    }

    /** Backwards-compat overload used by State.produceTick which doesn't have
     *  access to the parsed positional types. */
    private static Field buildField(String name, Object v) {
        return buildField(name, v, null);
    }

    /** Walk into nested lists to compute the deepest scalar type. */
    private static ArrowType listOrScalarType(Object v) {
        if (v instanceof List<?>) return new ArrowType.List();
        return inferArrowType(v);
    }

    private static List<Field> listChildren(Object v, String childName) {
        if (!(v instanceof List<?> l)) return null;
        if (l.isEmpty()) {
            // Empty list: we can't infer the element type from the runtime
            // value. Default to int64. DuckDB casts the projected column
            // back to whatever it expected, so the wire type only needs to
            // be self-consistent (List<int64>).
            return List.of(new Field(childName,
                    new FieldType(true, Schemas.INT64, null), null));
        }
        Object first = l.get(0);
        return List.of(new Field(childName,
                new FieldType(true, listOrScalarType(first), null), listChildren(first, "item")));
    }

    @Override public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        List<Object> values = new ArrayList<>(params.arguments().positional().subList(
                1, params.arguments().positional().size()));
        State state = new State((int) count, values);
        state.outputSchemaIpc = farm.query.vgi.internal.SchemaUtil.serializeSchema(params.outputSchema());
        return state;
    }

    private static ArrowType inferArrowType(Object v) {
        if (v == null) return Schemas.INT64;
        if (v instanceof Boolean) return Schemas.BOOL;
        if (v instanceof Long || v instanceof Integer || v instanceof Short || v instanceof Byte) {
            return Schemas.INT64;
        }
        if (v instanceof Double || v instanceof Float) return Schemas.FLOAT64;
        if (v instanceof byte[]) return new ArrowType.Binary();
        if (v instanceof java.util.Map) return new ArrowType.Struct();
        return Schemas.UTF8;
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int total;
        public List<Object> values;
        public boolean done;
        public byte[] outputSchemaIpc;

        public State() {}
        State(int total, List<Object> values) { this.total = total; this.values = values; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            // Prefer the bind-time output schema so user-supplied types like
            // TINYINT or FLOAT survive the round-trip. Falls back to inference
            // from the Java runtime values if the schema isn't available.
            Schema schema;
            if (outputSchemaIpc != null && outputSchemaIpc.length > 0) {
                schema = farm.query.vgi.internal.SchemaUtil.deserializeSchema(outputSchemaIpc);
            } else {
                List<Field> fields = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    fields.add(buildField("col_" + i, values.get(i)));
                }
                schema = new Schema(fields);
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            for (int c = 0; c < values.size(); c++) {
                FieldVector v = root.getVector(c);
                Object val = values.get(c);
                for (int r = 0; r < total; r++) setCell(v, r, val);
            }
            root.setRowCount(total);
            out.emit(root);
            out.finish();
        }

        @SuppressWarnings("unchecked")
        private static void setCell(FieldVector v, int row, Object val) {
            if (val == null) { v.setNull(row); return; }
            if (v instanceof BigIntVector bv) {
                bv.setSafe(row, ((Number) val).longValue());
            } else if (v instanceof org.apache.arrow.vector.IntVector iv) {
                iv.setSafe(row, ((Number) val).intValue());
            } else if (v instanceof org.apache.arrow.vector.SmallIntVector sv) {
                sv.setSafe(row, ((Number) val).shortValue());
            } else if (v instanceof org.apache.arrow.vector.TinyIntVector tv) {
                tv.setSafe(row, ((Number) val).byteValue());
            } else if (v instanceof org.apache.arrow.vector.UInt8Vector uv) {
                uv.setSafe(row, ((Number) val).longValue());
            } else if (v instanceof org.apache.arrow.vector.UInt4Vector uv) {
                uv.setSafe(row, ((Number) val).intValue());
            } else if (v instanceof org.apache.arrow.vector.UInt2Vector uv) {
                uv.setSafe(row, (char) ((Number) val).intValue());
            } else if (v instanceof org.apache.arrow.vector.UInt1Vector uv) {
                uv.setSafe(row, ((Number) val).byteValue());
            } else if (v instanceof Float8Vector fv) {
                fv.setSafe(row, ((Number) val).doubleValue());
            } else if (v instanceof org.apache.arrow.vector.Float4Vector fv) {
                fv.setSafe(row, ((Number) val).floatValue());
            } else if (v instanceof org.apache.arrow.vector.DecimalVector dv) {
                java.math.BigDecimal bd = val instanceof java.math.BigDecimal
                        ? (java.math.BigDecimal) val
                        : new java.math.BigDecimal(val.toString());
                dv.setSafe(row, bd.setScale(dv.getScale(), java.math.RoundingMode.HALF_UP));
            } else if (v instanceof org.apache.arrow.vector.Decimal256Vector dv) {
                java.math.BigDecimal bd = val instanceof java.math.BigDecimal
                        ? (java.math.BigDecimal) val
                        : new java.math.BigDecimal(val.toString());
                dv.setSafe(row, bd.setScale(dv.getScale(), java.math.RoundingMode.HALF_UP));
            } else if (v instanceof BitVector bit) {
                bit.setSafe(row, (Boolean) val ? 1 : 0);
            } else if (v instanceof VarCharVector vc) {
                vc.setSafe(row, new Text(val.toString()));
            } else if (v instanceof VarBinaryVector vb) {
                vb.setSafe(row, (byte[]) val);
            } else if (v instanceof org.apache.arrow.vector.complex.ListVector lv) {
                writeListAt(lv, row, (List<Object>) val);
            } else if (v instanceof org.apache.arrow.vector.complex.StructVector sv) {
                java.util.Map<String, Object> map = (java.util.Map<String, Object>) val;
                for (org.apache.arrow.vector.types.pojo.Field f : sv.getField().getChildren()) {
                    setCell(sv.getChild(f.getName()), row, map.get(f.getName()));
                }
                sv.setIndexDefined(row);
            }
        }

        private static void writeListAt(org.apache.arrow.vector.complex.ListVector lv,
                                          int row, List<Object> list) {
            org.apache.arrow.vector.complex.impl.UnionListWriter w = lv.getWriter();
            w.setPosition(row);
            w.startList();
            for (Object item : list) {
                writeListItem((org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter) w, item, lv.getAllocator());
            }
            w.endList();
        }

        @SuppressWarnings("unchecked")
        private static void writeListItem(org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter w,
                                            Object item, org.apache.arrow.memory.BufferAllocator alloc) {
            if (item == null) { w.writeNull(); return; }
            if (item instanceof Long l) w.bigInt().writeBigInt(l);
            else if (item instanceof Integer i) w.bigInt().writeBigInt(i.longValue());
            else if (item instanceof Double d) w.float8().writeFloat8(d);
            else if (item instanceof Boolean b) w.bit().writeBit(b ? 1 : 0);
            else if (item instanceof String s) {
                byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                try (org.apache.arrow.memory.ArrowBuf buf = alloc.buffer(bytes.length)) {
                    buf.setBytes(0, bytes);
                    w.varChar().writeVarChar(0, bytes.length, buf);
                }
            }
            else if (item instanceof List<?>) {
                org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter inner = w.list();
                inner.startList();
                for (Object child : (List<Object>) item) writeListItem(inner, child, alloc);
                inner.endList();
            }
        }
    }
}
