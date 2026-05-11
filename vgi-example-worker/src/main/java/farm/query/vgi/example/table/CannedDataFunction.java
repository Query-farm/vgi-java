// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code _table_data(table_name VARCHAR [const]) -> table} — generic
 * data-backed scan for example.data.* catalog tables. The schema is selected
 * at bind time based on the {@code table_name} argument, and the canned data
 * is emitted in {@code produceTick}. Keeps the integration tests honest
 * without porting one TableFunction per fixture table.
 */
public final class CannedDataFunction implements TableFunction {

    @Override public String name() { return "_table_data"; }

    @Override public FunctionMetadata metadata() {
        // Virtual columns (DuckDB's rowid mapping onto an is_row_id field)
        // require projection pushdown to be advertised on the function.
        return FunctionMetadata.describe("Canned data fixture (test scaffolding)")
                .withPushdown(/*projection=*/true, /*filter=*/false, /*autoApply=*/false);
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(new ArgSpec("table_name", 0, Schemas.UTF8, /*isConst=*/true));
    }

    @Override public BindResponse onBind(TableBindParams p) {
        Object tnObj = p.arguments().positional().isEmpty()
                ? null : p.arguments().positionalAt(0);
        if (tnObj == null) {
            // Catalog enumeration / function listing — no argument supplied;
            // return a placeholder schema so the function is discoverable.
            return BindResponse.forSchema(SchemaUtil.serializeSchema(
                    new Schema(List.of(f("placeholder", Schemas.INT64, true)))));
        }
        String tn = (String) tnObj;
        Schema s = SCHEMAS.get(tn);
        if (s == null) {
            throw new IllegalArgumentException("CannedData: unknown table " + tn);
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(s));
    }

    @Override public long cardinality(TableBindParams p) {
        String tn = (String) p.arguments().positionalAt(0);
        Object[][] rows = ROWS.get(tn);
        return rows == null ? -1 : rows.length;
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        String tn = (String) params.arguments().positionalAt(0);
        return new State(tn, params.projectionIds());
    }

    /** Schema definitions per table name. */
    private static final Map<String, Schema> SCHEMAS = new HashMap<>();
    /** Rows per table name. Each Object[] maps to one row, indexed by field order. */
    private static final Map<String, Object[][]> ROWS = new HashMap<>();

    private static Field f(String name, ArrowType type, boolean nullable) {
        return new Field(name, new FieldType(nullable, type, null), null);
    }

    static {
        SCHEMAS.put("products", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("name", Schemas.UTF8, false),
                f("price", Schemas.FLOAT64, false),
                f("quantity", Schemas.INT64, true))));
        ROWS.put("products", new Object[][]{
                {1L, "Widget", 9.99d, 100L},
                {2L, "Gadget", 24.99d, 50L},
                {3L, "Doohickey", 4.99d, 200L}});

        SCHEMAS.put("departments", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("name", Schemas.UTF8, false),
                f("budget", Schemas.FLOAT64, true))));
        ROWS.put("departments", new Object[][]{
                {1L, "Engineering", 500000.0d},
                {2L, "Sales", 300000.0d},
                {3L, "HR", 200000.0d}});

        SCHEMAS.put("employees", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("name", Schemas.UTF8, false),
                f("email", Schemas.UTF8, false),
                f("department_id", Schemas.INT64, true))));
        ROWS.put("employees", new Object[][]{
                {1L, "Alice", "alice@co.com", 1L},
                {2L, "Bob",   "bob@co.com",   2L},
                {3L, "Carol", "carol@co.com", 1L},
                {4L, "Dave",  "dave@co.com",  2L},
                {5L, "Eve",   "eve@co.com",   3L}});

        SCHEMAS.put("colors", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("color", Schemas.UTF8, false),
                f("hex_code", Schemas.UTF8, false))));
        ROWS.put("colors", new Object[][]{
                {1L, "blue",  "#0000FF"},
                {2L, "green", "#00FF00"},
                {3L, "red",   "#FF0000"}});

        SCHEMAS.put("generated_sequence", new Schema(List.of(
                f("n", Schemas.INT64, true))));
        Object[][] genRows = new Object[10][];
        for (int i = 0; i < 10; i++) genRows[i] = new Object[]{(long) i};
        ROWS.put("generated_sequence", genRows);

        // rowid_* fixtures: 20 rows each. The row_id column is *physical*
        // (declared in the schema), even though duckdb hides it via SELECT *.
        SCHEMAS.put("rowid_first", new Schema(List.of(
                f("row_id", Schemas.INT64, false),
                f("name", Schemas.UTF8, true),
                f("value", Schemas.UTF8, true))));
        Object[][] rf = new Object[20][];
        for (int i = 0; i < 20; i++) rf[i] = new Object[]{(long) i, "item_" + i, "val_" + i};
        ROWS.put("rowid_first", rf);

        SCHEMAS.put("rowid_middle", new Schema(List.of(
                f("name", Schemas.UTF8, true),
                f("row_id", Schemas.INT64, false),
                f("value", Schemas.UTF8, true))));
        Object[][] rm = new Object[20][];
        for (int i = 0; i < 20; i++) rm[i] = new Object[]{"item_" + i, (long) i, "val_" + i};
        ROWS.put("rowid_middle", rm);

        SCHEMAS.put("rowid_last", new Schema(List.of(
                f("name", Schemas.UTF8, true),
                f("value", Schemas.UTF8, true),
                f("row_id", Schemas.INT64, false))));
        Object[][] rl = new Object[20][];
        for (int i = 0; i < 20; i++) rl[i] = new Object[]{"item_" + i, "val_" + i, (long) i};
        ROWS.put("rowid_last", rl);

        SCHEMAS.put("rowid_string", new Schema(List.of(
                f("row_id", Schemas.UTF8, false),
                f("payload", Schemas.UTF8, true))));
        Object[][] rs = new Object[20][];
        for (int i = 0; i < 20; i++) rs[i] = new Object[]{"rid_" + i, "data_" + i};
        ROWS.put("rowid_string", rs);

        // rowid_struct: row_id is a STRUCT<a: bigint, b: utf8>.
        Field rsRowId = new Field("row_id",
                new FieldType(false, new ArrowType.Struct(), null),
                List.of(
                        new Field("a", new FieldType(true, Schemas.INT64, null), null),
                        new Field("b", new FieldType(true, Schemas.UTF8, null), null)));
        SCHEMAS.put("rowid_struct", new Schema(List.of(
                rsRowId,
                f("payload", Schemas.UTF8, true))));
        Object[][] rstr = new Object[20][];
        for (int i = 0; i < 20; i++) {
            Map<String, Object> structVal = new HashMap<>();
            structVal.put("a", (long) i);
            structVal.put("b", "s_" + i);
            rstr[i] = new Object[]{structVal, "p_" + i};
        }
        ROWS.put("rowid_struct", rstr);

        // versioned_data: schema evolution across versions
        //   v1: (id)                          — 3 rows
        //   v2: (id, name, score, active)     — 5 rows
        //   v3: (id, score)                   — 4 rows  [default]
        SCHEMAS.put("versioned_data_v1", new Schema(List.of(
                f("id", Schemas.INT64, false))));
        ROWS.put("versioned_data_v1", new Object[][]{
                {1L}, {2L}, {3L}});

        SCHEMAS.put("versioned_data_v2", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("name", Schemas.UTF8, true),
                f("score", Schemas.FLOAT64, true),
                f("active", Schemas.BOOL, true))));
        ROWS.put("versioned_data_v2", new Object[][]{
                {1L, "alice", 10.0d, true},
                {2L, "bob",   20.0d, false},
                {3L, "carol", 30.0d, true},
                {4L, "dave",  40.0d, false},
                {5L, "eve",   50.0d, true}});

        SCHEMAS.put("versioned_data_v3", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("score", Schemas.FLOAT64, true))));
        ROWS.put("versioned_data_v3", new Object[][]{
                {1L, 15.0d}, {2L, 25.0d}, {3L, 35.0d}, {4L, 45.0d}});

        SCHEMAS.put("versioned_data", SCHEMAS.get("versioned_data_v3"));
        ROWS.put("versioned_data", ROWS.get("versioned_data_v3"));

        // versioned_constraints: constraint evolution
        SCHEMAS.put("versioned_constraints_v1", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("name", Schemas.UTF8, true))));
        ROWS.put("versioned_constraints_v1", new Object[][]{
                {1L, "Alice"}, {2L, "Bob"}});

        SCHEMAS.put("versioned_constraints_v2", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("name", Schemas.UTF8, false),
                f("email", Schemas.UTF8, false))));
        ROWS.put("versioned_constraints_v2", new Object[][]{
                {1L, "Alice", "a@co"},
                {2L, "Bob",   "b@co"},
                {3L, "Carol", "c@co"}});

        SCHEMAS.put("versioned_constraints_v3", new Schema(List.of(
                f("id", Schemas.INT64, false),
                f("name", Schemas.UTF8, false),
                f("email", Schemas.UTF8, false),
                f("department_id", Schemas.INT64, true))));
        ROWS.put("versioned_constraints_v3", new Object[][]{
                {1L, "Alice", "a@co", 1L},
                {2L, "Bob",   "b@co", 2L},
                {3L, "Carol", "c@co", 1L}});

        SCHEMAS.put("versioned_constraints", SCHEMAS.get("versioned_constraints_v3"));
        ROWS.put("versioned_constraints", ROWS.get("versioned_constraints_v3"));

        // Animals — exposed by the versioned_tables worker; schema evolves
        // across data versions (v1.1.0 + v2.0.0 add a color column).
        SCHEMAS.put("animals_v_1_0_0", new Schema(List.of(
                f("name", Schemas.UTF8, true),
                f("legs", Schemas.INT64, true),
                f("sound", Schemas.UTF8, true))));
        ROWS.put("animals_v_1_0_0", new Object[][]{
                {"chicken", 2L, "cluck"},
                {"cow",     4L, "moo"},
                {"horse",   4L, "neigh"},
                {"pig",     4L, "oink"},
                {"sheep",   4L, "baa"}});

        SCHEMAS.put("animals_v_1_1_0", new Schema(List.of(
                f("name", Schemas.UTF8, true),
                f("legs", Schemas.INT64, true),
                f("sound", Schemas.UTF8, true),
                f("color", Schemas.UTF8, true))));
        ROWS.put("animals_v_1_1_0", new Object[][]{
                {"chicken", 2L, "cluck", "white"},
                {"cow",     4L, "moo",   "brown"},
                {"horse",   4L, "neigh", "black"}});

        SCHEMAS.put("animals_v_2_0_0", new Schema(List.of(
                f("name", Schemas.UTF8, true),
                f("legs", Schemas.INT64, true),
                f("sound", Schemas.UTF8, true),
                f("color", Schemas.UTF8, true))));
        ROWS.put("animals_v_2_0_0", new Object[][]{
                {"chicken", 2L, "cluck", "white"},
                {"cow",     4L, "moo",   "brown"},
                {"horse",   4L, "neigh", "black"},
                {"pig",     4L, "oink",  "pink"},
                {"sheep",   4L, "baa",   "white"}});

        SCHEMAS.put("animals", SCHEMAS.get("animals_v_1_0_0"));
        ROWS.put("animals", ROWS.get("animals_v_1_0_0"));

        // Plants — only available at data version 2.0.0+.
        Schema plantsSchema = new Schema(List.of(
                f("name", Schemas.UTF8, true),
                f("kind", Schemas.UTF8, true),
                f("height_m", Schemas.FLOAT64, true)));
        Object[][] plantsRows = new Object[][]{
                {"oak",    "tree",     20.0d},
                {"pine",   "tree",     25.0d},
                {"rose",   "flower",    0.6d},
                {"tomato", "vegetable", 1.5d},
                {"wheat",  "grass",     1.0d}};
        SCHEMAS.put("plants", plantsSchema);
        SCHEMAS.put("plants_v_2_0_0", plantsSchema);
        SCHEMAS.put("plants_v_3_0_0", plantsSchema);
        ROWS.put("plants", plantsRows);
        ROWS.put("plants_v_2_0_0", plantsRows);
        ROWS.put("plants_v_3_0_0", plantsRows);

        SCHEMAS.put("funny_numbers", new Schema(List.of(
                f("n", Schemas.INT64, true))));
        Object[][] fn = new Object[123456][];
        for (int i = 0; i < 123456; i++) fn[i] = new Object[]{(long) i};
        ROWS.put("funny_numbers", fn);

        SCHEMAS.put("volatile_numbers", new Schema(List.of(
                f("value", Schemas.INT64, true))));
        Object[][] vn = new Object[100][];
        for (int i = 0; i < 100; i++) vn[i] = new Object[]{(long) i};
        ROWS.put("volatile_numbers", vn);

        SCHEMAS.put("projects", new Schema(List.of(
                f("department_id", Schemas.INT64, false),
                f("project_code", Schemas.UTF8, false),
                f("title", Schemas.UTF8, false))));
        ROWS.put("projects", new Object[][]{
                {1L, "P001", "Backend API"},
                {1L, "P002", "Frontend UI"},
                {2L, "P003", "Sales Portal"}});
    }

    public static boolean has(String tableName) { return SCHEMAS.containsKey(tableName); }
    public static Schema schemaFor(String tableName) { return SCHEMAS.get(tableName); }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String tableName;
        public List<Integer> projectionIds;
        public boolean done;
        public State() {}
        State(String tn, List<Integer> projectionIds) {
            this.tableName = tn;
            this.projectionIds = projectionIds == null ? List.of() : projectionIds;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            Schema fullSchema = SCHEMAS.get(tableName);
            Object[][] rows = ROWS.get(tableName);
            if (fullSchema == null || rows == null) { out.finish(); return; }
            // Apply projection: emit only the requested column subset.
            List<Field> outFields = new ArrayList<>();
            int[] colMap;
            if (projectionIds == null || projectionIds.isEmpty()) {
                colMap = new int[fullSchema.getFields().size()];
                for (int i = 0; i < colMap.length; i++) {
                    colMap[i] = i;
                    outFields.add(fullSchema.getFields().get(i));
                }
            } else {
                colMap = new int[projectionIds.size()];
                for (int i = 0; i < projectionIds.size(); i++) {
                    int origIdx = projectionIds.get(i);
                    colMap[i] = origIdx;
                    outFields.add(fullSchema.getFields().get(origIdx));
                }
            }
            Schema outSchema = new Schema(outFields);
            BufferAllocator alloc = Allocators.root();
            VectorSchemaRoot root = VectorSchemaRoot.create(outSchema, alloc);
            root.allocateNew();
            for (int outCol = 0; outCol < outFields.size(); outCol++) {
                FieldVector v = root.getVector(outCol);
                int srcCol = colMap[outCol];
                for (int r = 0; r < rows.length; r++) {
                    Object val = rows[r][srcCol];
                    setCell(v, r, val);
                }
            }
            root.setRowCount(rows.length);
            out.emit(root);
            out.finish();
        }
    }

    @SuppressWarnings("unchecked")
    private static void setCell(FieldVector v, int row, Object val) {
        if (val == null) { v.setNull(row); return; }
        if (v instanceof BigIntVector bv) {
            bv.setSafe(row, ((Number) val).longValue());
        } else if (v instanceof Float8Vector fv) {
            fv.setSafe(row, ((Number) val).doubleValue());
        } else if (v instanceof org.apache.arrow.vector.BitVector bit) {
            bit.setSafe(row, ((Boolean) val) ? 1 : 0);
        } else if (v instanceof VarCharVector vc) {
            vc.setSafe(row, new Text(val.toString()));
        } else if (v instanceof StructVector sv) {
            Map<String, Object> entries = (Map<String, Object>) val;
            NullableStructWriter w = sv.getWriter();
            w.setPosition(row);
            w.start();
            for (Map.Entry<String, Object> e : entries.entrySet()) {
                Object cell = e.getValue();
                if (cell instanceof Long l) {
                    w.bigInt(e.getKey()).writeBigInt(l);
                } else if (cell instanceof String s) {
                    byte[] bytes = s.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                    try (org.apache.arrow.memory.ArrowBuf buf = sv.getAllocator().buffer(bytes.length)) {
                        buf.setBytes(0, bytes);
                        w.varChar(e.getKey()).writeVarChar(0, bytes.length, buf);
                    }
                }
            }
            w.end();
            sv.setIndexDefined(row);
        }
    }
}
