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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.complex.impl.NullableStructWriter;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * {@code rowid_sequence(count BIGINT [const], layout VARCHAR := 'first' [const],
 *                       row_id_type VARCHAR := 'int64' [const])} — sequence with
 * a row_id-tagged column whose position and type are configurable. Used by
 * {@code rowid.test} for argument validation regression checks.
 */
public final class RowIdSequenceFunction implements TableFunction {

    private static final java.util.Set<String> LAYOUTS =
            java.util.Set.of("first", "middle", "last");
    private static final java.util.Set<String> ROW_ID_TYPES =
            java.util.Set.of("int64", "string", "struct");

    @Override public String name() { return "rowid_sequence"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Sequence with row_id column")
                .withPushdown(/*projection=*/true, /*filter=*/false, /*autoApply=*/false);
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("count", 0, Schemas.INT64, /*isConst=*/true),
                ArgSpec.named("layout", Schemas.UTF8, "first"),
                ArgSpec.named("row_id_type", Schemas.UTF8, "int64"));
    }

    @Override public BindResponse onBind(TableBindParams p) {
        String layout = stringArg(p, "layout", "first");
        String rowIdType = stringArg(p, "row_id_type", "int64");
        if (!LAYOUTS.contains(layout)) {
            throw new IllegalArgumentException("layout must be one of the allowed choices [first, middle, last], got '" + layout + "'");
        }
        if (!ROW_ID_TYPES.contains(rowIdType)) {
            throw new IllegalArgumentException("row_id_type must be one of the allowed choices [int64, string, struct], got '" + rowIdType + "'");
        }
        return BindResponse.forSchema(farm.query.vgi.internal.SchemaUtil.serializeSchema(
                buildSchema(layout, rowIdType)));
    }

    @Override public long cardinality(TableBindParams p) {
        Object c = p.arguments().positionalAt(0);
        return c instanceof Number n ? n.longValue() : -1L;
    }

    @Override public TableProducerState createProducer(TableInitParams p) {
        long count = ((Number) p.arguments().positionalAt(0)).longValue();
        String layout = namedString(p.arguments().named(), "layout", "first");
        String rowIdType = namedString(p.arguments().named(), "row_id_type", "int64");
        return new State((int) count, layout, rowIdType);
    }

    private static String stringArg(TableBindParams p, String name, String defaultValue) {
        Object v = p.arguments().named().get(name);
        if (v == null) return defaultValue;
        return v.toString();
    }

    private static String namedString(Map<String, Object> named, String name, String defaultValue) {
        Object v = named.get(name);
        return v == null ? defaultValue : v.toString();
    }

    private static Schema buildSchema(String layout, String rowIdType) {
        Map<String, String> rowIdMeta = Map.of("is_row_id", "true");
        ArrowType ridType = switch (rowIdType) {
            case "string" -> Schemas.UTF8;
            case "struct" -> new ArrowType.Struct();
            default -> Schemas.INT64;
        };
        List<Field> ridChildren = "struct".equals(rowIdType)
                ? List.of(
                        Schemas.nullable("a", Schemas.INT64),
                        Schemas.nullable("b", Schemas.UTF8))
                : List.of();
        Field rid = new Field("row_id",
                new FieldType(false, ridType, null, rowIdMeta), ridChildren);
        Field name = Schemas.nullable("name", Schemas.UTF8);
        Field value = Schemas.nullable("value", Schemas.UTF8);
        List<Field> fields = switch (layout) {
            case "middle" -> List.of(name, rid, value);
            case "last"   -> List.of(name, value, rid);
            default       -> List.of(rid, name, value);
        };
        return new Schema(fields);
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int total;
        public String layout;
        public String rowIdType;
        public boolean done;

        public State() {}
        State(int total, String layout, String rowIdType) {
            this.total = total; this.layout = layout; this.rowIdType = rowIdType;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            if (total <= 0) { out.finish(); return; }
            Schema schema = buildSchema(layout, rowIdType);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            for (Field f : schema.getFields()) {
                if ("row_id".equals(f.getName())) {
                    fillRowIds(root, f, rowIdType, total);
                } else if ("name".equals(f.getName())) {
                    VarCharVector v = (VarCharVector) root.getVector("name");
                    for (int i = 0; i < total; i++) v.setSafe(i, new Text("item_" + i));
                } else if ("value".equals(f.getName())) {
                    VarCharVector v = (VarCharVector) root.getVector("value");
                    for (int i = 0; i < total; i++) v.setSafe(i, new Text("val_" + i));
                }
            }
            root.setRowCount(total);
            out.emit(root);
            out.finish();
        }

        private static void fillRowIds(VectorSchemaRoot root, Field f, String rowIdType, int total) {
            switch (rowIdType) {
                case "string" -> {
                    VarCharVector v = (VarCharVector) root.getVector("row_id");
                    for (int i = 0; i < total; i++) v.setSafe(i, new Text("rid_" + i));
                }
                case "struct" -> {
                    StructVector sv = (StructVector) root.getVector("row_id");
                    NullableStructWriter w = sv.getWriter();
                    for (int i = 0; i < total; i++) {
                        w.setPosition(i);
                        w.start();
                        w.bigInt("a").writeBigInt(i);
                        byte[] bytes = ("s_" + i).getBytes(java.nio.charset.StandardCharsets.UTF_8);
                        try (org.apache.arrow.memory.ArrowBuf buf =
                                sv.getAllocator().buffer(bytes.length)) {
                            buf.setBytes(0, bytes);
                            w.varChar("b").writeVarChar(0, bytes.length, buf);
                        }
                        w.end();
                        sv.setIndexDefined(i);
                    }
                }
                default -> {
                    BigIntVector v = (BigIntVector) root.getVector("row_id");
                    for (int i = 0; i < total; i++) v.setSafe(i, i);
                }
            }
        }
    }
}
