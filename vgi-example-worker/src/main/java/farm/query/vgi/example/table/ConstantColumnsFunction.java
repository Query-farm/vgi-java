// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionSpec;
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

    private static final FunctionSpec SPEC = FunctionSpec.builder("constant_columns")
            .description("Generates rows with constant values from varargs")
            .constArg("count", Schemas.INT64)
            .arg(new ArgSpec("values", 1, new ArrowType.Null(), "", true, false, "",
                    List.of(), /*varargs=*/true, /*anyType=*/true))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableBindParams p) {
        List<Object> positionals = p.arguments().positional();
        // positionals[0] is count, positionals[1..] are the varargs values.
        List<Field> fields = new ArrayList<>();
        for (int i = 1; i < positionals.size(); i++) {
            ArrowType wireType = p.arguments().positionalTypeAt(i);
            Field argField = p.arguments().positionalFieldAt(i);
            Object val = positionals.get(i);
            fields.add(buildOutputField("col_" + (i - 1), val, wireType, argField));
        }
        if (fields.isEmpty()) {
            // Catalog enumeration with no varargs supplied — placeholder.
            fields.add(Schemas.nullable("placeholder", Schemas.INT64));
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(
                new Schema(fields)));
    }

    /**
     * Pick the output Field for a varargs value at bind time.
     *
     * <p>Strategy by precedence:
     * <ol>
     *   <li>ENUM (dict-encoded source field): {@link ArgumentsParser} has
     *       already resolved the index byte to its underlying value, so
     *       infer the output type from the resolved Java value — we can't
     *       reproduce a dict batch on the producer path.</li>
     *   <li>Compound types (Struct / List / Map / FixedSizeList): reuse
     *       the source Field's structure verbatim under the new
     *       {@code col_N} name. Inferring from runtime values widens
     *       INTEGER[] to BIGINT[], drops Map identity, and misses the
     *       entries:struct<key,value> shape Arrow Map requires.</li>
     *   <li>Lossless-tagged scalars (arrow.opaque, arrow.uuid, …):
     *       preserve the source Field's extension metadata so DuckDB
     *       rebinds the SQL type (HUGEINT, UUID, …) rather than seeing a
     *       generic FixedSizeBinary.</li>
     *   <li>Plain scalars: use the bind-time {@code wireType} directly
     *       (TINYINT vs BIGINT, FLOAT vs DOUBLE, …).</li>
     *   <li>No type info at all: fall back to {@code buildField}'s
     *       runtime-class inference.</li>
     * </ol>
     */
    private static Field buildOutputField(String name, Object v, ArrowType wireType, Field argField) {
        if (argField != null && argField.getDictionary() != null) {
            return buildField(name, v, null);
        }
        if (argField != null && (argField.getType() instanceof ArrowType.Struct
                || argField.getType() instanceof ArrowType.List
                || argField.getType() instanceof ArrowType.FixedSizeList
                || argField.getType() instanceof ArrowType.Map)) {
            return new Field(name,
                    new FieldType(true, argField.getType(), argField.getDictionary(),
                            argField.getMetadata()),
                    argField.getChildren());
        }
        if (argField != null && argField.getMetadata() != null
                && !argField.getMetadata().isEmpty()) {
            return new Field(name,
                    new FieldType(true, argField.getType(), argField.getDictionary(),
                            argField.getMetadata()),
                    argField.getChildren());
        }
        return buildField(name, v, wireType);
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
        // Recurse via buildField so nested Map elements (structs inside a
        // list) get their child fields populated, not left as empty
        // structs that DuckDB rejects with "no fields".
        return List.of(buildField(childName, first, null));
    }

    @Override public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        long count = ((Number) params.arguments().positionalAt(0)).longValue();
        List<Object> values = new ArrayList<>(params.arguments().positional().subList(
                1, params.arguments().positional().size()));
        State state = new State(count, values);
        state.outputSchemaIpc = SchemaUtil.serializeSchema(params.outputSchema());
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
        private static final long serialVersionUID = 2L;
        /** Rows per emitted batch; matches DuckDB's standard vector size so a
         *  large {@code count} streams instead of materializing in one root. */
        private static final int BATCH_SIZE = 2048;
        public long total;
        public long emitted;
        public List<Object> values;
        public byte[] outputSchemaIpc;
        private transient Schema cachedSchema;

        public State() {}
        State(long total, List<Object> values) { this.total = total; this.values = values; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted >= total) { out.finish(); return; }
            Schema schema = schema();
            int n = (int) Math.min(BATCH_SIZE, total - emitted);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            boolean ownershipTransferred = false;
            try {
                root.allocateNew();
                for (int c = 0; c < values.size(); c++) {
                    fillColumn(root.getVector(c), values.get(c), n);
                }
                root.setRowCount(n);
                out.emit(root);
                ownershipTransferred = true;
            } finally {
                if (!ownershipTransferred) root.close();
            }
            emitted += n;
        }

        /** Resolve (and cache) the output schema. Prefer the bind-time output
         *  schema so user-supplied types like TINYINT or FLOAT survive the
         *  round-trip; fall back to inference from the runtime values. */
        private Schema schema() {
            if (cachedSchema != null) return cachedSchema;
            if (outputSchemaIpc != null && outputSchemaIpc.length > 0) {
                cachedSchema = SchemaUtil.deserializeSchema(outputSchemaIpc);
            } else {
                List<Field> fields = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    fields.add(buildField("col_" + i, values.get(i)));
                }
                cachedSchema = new Schema(fields);
            }
            return cachedSchema;
        }

        /** Fill {@code rows} cells of {@code v} with the constant {@code val},
         *  resolving the concrete vector type once instead of per row. Nested /
         *  uncommon types fall back to the per-row {@link #setCell} dispatch. */
        private static void fillColumn(FieldVector v, Object val, int rows) {
            if (val == null) {
                for (int r = 0; r < rows; r++) v.setNull(r);
                return;
            }
            if (v instanceof BigIntVector bv) {
                long x = ((Number) val).longValue();
                bv.allocateNew(rows);
                for (int r = 0; r < rows; r++) bv.set(r, x);
            } else if (v instanceof org.apache.arrow.vector.IntVector iv) {
                int x = ((Number) val).intValue();
                iv.allocateNew(rows);
                for (int r = 0; r < rows; r++) iv.set(r, x);
            } else if (v instanceof Float8Vector fv) {
                double x = ((Number) val).doubleValue();
                fv.allocateNew(rows);
                for (int r = 0; r < rows; r++) fv.set(r, x);
            } else if (v instanceof org.apache.arrow.vector.Float4Vector fv) {
                float x = ((Number) val).floatValue();
                fv.allocateNew(rows);
                for (int r = 0; r < rows; r++) fv.set(r, x);
            } else if (v instanceof BitVector bit) {
                int x = (Boolean) val ? 1 : 0;
                bit.allocateNew(rows);
                for (int r = 0; r < rows; r++) bit.set(r, x);
            } else if (v instanceof VarCharVector vc) {
                byte[] x = val.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);
                vc.allocateNew((long) x.length * rows, rows);
                for (int r = 0; r < rows; r++) vc.setSafe(r, x, 0, x.length);
            } else {
                // Smaller ints, decimals, binary, list/struct/map, temporal —
                // per-row dispatch (rarer paths; correctness over speed).
                for (int r = 0; r < rows; r++) setCell(v, r, val);
            }
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
                // UInt2Vector.getObject returns Character (uint16 = char range);
                // ArgumentsParser stores it as-is, so handle either Character or Number.
                int n = val instanceof Character c ? c.charValue() : ((Number) val).intValue();
                uv.setSafe(row, (char) n);
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
                bd = bd.setScale(dv.getScale(), java.math.RoundingMode.HALF_UP);
                if (bd.precision() > dv.getPrecision()) {
                    // HUGEINT max (2^127-1) has 39 digits but Arrow Decimal128
                    // declares precision 38. Bypass the precision check by
                    // writing the raw 16-byte big-endian payload directly.
                    dv.setBigEndianSafe(row, toFixedBigEndian(bd.unscaledValue(), 16));
                } else {
                    dv.setSafe(row, bd);
                }
            } else if (v instanceof org.apache.arrow.vector.Decimal256Vector dv) {
                java.math.BigDecimal bd = val instanceof java.math.BigDecimal
                        ? (java.math.BigDecimal) val
                        : new java.math.BigDecimal(val.toString());
                bd = bd.setScale(dv.getScale(), java.math.RoundingMode.HALF_UP);
                if (bd.precision() > dv.getPrecision()) {
                    dv.setBigEndianSafe(row, toFixedBigEndian(bd.unscaledValue(), 32));
                } else {
                    dv.setSafe(row, bd);
                }
            } else if (v instanceof BitVector bit) {
                bit.setSafe(row, (Boolean) val ? 1 : 0);
            } else if (v instanceof VarCharVector vc) {
                vc.setSafe(row, new Text(val.toString()));
            } else if (v instanceof VarBinaryVector vb) {
                vb.setSafe(row, (byte[]) val);
            } else if (v instanceof org.apache.arrow.vector.FixedSizeBinaryVector fb) {
                // Lossless-tagged DuckDB types (HUGEINT → arrow.opaque,
                // UHUGEINT → arrow.opaque, UUID → arrow.uuid, etc.) arrive
                // as raw byte[] payloads. Pass them through unchanged.
                fb.setSafe(row, (byte[]) val);
            } else {
                // List / Struct / Date / Time / Interval / etc.
                // VectorScalarCodec.write dispatches on the destination
                // vector type, so e.g. a Long item lands as int32 when
                // the inner is IntVector — UnionListWriter would have
                // mis-routed it through .bigInt().
                farm.query.vgi.internal.VectorScalarCodec.write(v, row, val);
            }
        }

        /**
         * Pack a {@link java.math.BigInteger} into a fixed-width big-endian
         * byte array (sign-extended on the left). Used to feed
         * {@code DecimalVector.setBigEndianSafe}, which doesn't validate
         * precision — needed for HUGEINT max ({@code 2^127-1}, 39 digits)
         * that doesn't fit the wire-declared {@code Decimal(38, 0)}.
         */
        private static byte[] toFixedBigEndian(java.math.BigInteger v, int width) {
            byte[] src = v.toByteArray();
            byte[] out = new byte[width];
            byte sign = (byte) (v.signum() < 0 ? 0xff : 0x00);
            java.util.Arrays.fill(out, sign);
            int copyLen = Math.min(width, src.length);
            int srcOff = src.length - copyLen;
            int dstOff = width - copyLen;
            System.arraycopy(src, srcOff, out, dstOff, copyLen);
            return out;
        }

        @SuppressWarnings({"unchecked"})
        private static void writeMapAt(org.apache.arrow.vector.complex.MapVector mv,
                                         int row, Object value) {
            org.apache.arrow.vector.complex.impl.UnionMapWriter w = mv.getWriter();
            w.setPosition(row);
            w.startMap();
            // Accept either a Map<K,V> or a List of {key, value} entry maps —
            // depending on whether the value came from a Java caller (Map)
            // or from MapVector.getObject() round-trip (List of entries).
            if (value instanceof java.util.Map<?, ?> m) {
                for (java.util.Map.Entry<?, ?> e : m.entrySet()) {
                    w.startEntry();
                    writeListItem(w.key(), e.getKey(), mv.getAllocator());
                    writeListItem(w.value(), e.getValue(), mv.getAllocator());
                    w.endEntry();
                }
            } else if (value instanceof java.util.List<?> entries) {
                for (Object e : entries) {
                    java.util.Map<String, Object> kv = (java.util.Map<String, Object>) e;
                    w.startEntry();
                    writeListItem(w.key(), kv.get("key"), mv.getAllocator());
                    writeListItem(w.value(), kv.get("value"), mv.getAllocator());
                    w.endEntry();
                }
            }
            w.endMap();
        }


        @SuppressWarnings("unchecked")
        private static void writeListItem(org.apache.arrow.vector.complex.writer.BaseWriter.ListWriter w,
                                            Object item, org.apache.arrow.memory.BufferAllocator alloc) {
            if (item == null) { w.writeNull(); return; }
            // Dispatch on Java type → matching Arrow writer width. Critical
            // for Map/List of int32 values: ArgumentsParser delivers Integer
            // (from IntVector.getObject), and writing as bigInt() would slot
            // into the wrong primitive width and yield 0.
            if (item instanceof Long l) w.bigInt().writeBigInt(l);
            else if (item instanceof Integer i) w.integer().writeInt(i);
            else if (item instanceof Short s) w.smallInt().writeSmallInt(s);
            else if (item instanceof Byte b) w.tinyInt().writeTinyInt(b);
            else if (item instanceof Double d) w.float8().writeFloat8(d);
            else if (item instanceof Float f) w.float4().writeFloat4(f);
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
