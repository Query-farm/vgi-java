// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
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
 * Two overloads of {@code repeat_value(count, values...)}:
 * <ul>
 *   <li>{@code (count BIGINT, BIGINT...)} — emits {@code count} rows of the
 *       given integer values, one column per varargs slot.</li>
 *   <li>{@code (count BIGINT, VARCHAR...)} — same, with string values.</li>
 * </ul>
 * Used by overload/table_varargs_overload.test.
 */
public final class RepeatValueFunctions {

    private RepeatValueFunctions() {}

    private static byte[] schemaIpc(int n, ArrowType t) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fields.add(new Field("v" + i, new FieldType(true, t, null), null));
        }
        return SchemaUtil.serializeSchema(new Schema(fields));
    }

    private static Schema schema(int n, ArrowType t) {
        List<Field> fields = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            fields.add(new Field("v" + i, new FieldType(true, t, null), null));
        }
        return new Schema(fields);
    }

    public static final class IntVariant implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("repeat_value")
                .description("Repeat integer values for N rows")
                .constArg("count", Schemas.INT64)
                .arg(new ArgSpec("values", 1, Schemas.INT64, "", true, false, "",
                        List.of(), /*varargs=*/true, /*anyType=*/false))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) {
            int n = Math.max(0, p.arguments().positional().size() - 1);
            return BindResponse.forSchema(schemaIpc(n, Schemas.INT64));
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long count = ex.positional(0, "count").asLong().required();
            List<Object> tail = ex.varargsFrom(1);
            long[] values = new long[tail.size()];
            for (int i = 0; i < tail.size(); i++) {
                values[i] = ((Number) tail.get(i)).longValue();
            }
            return new IntState((int) count, values, schema(tail.size(), Schemas.INT64));
        }
    }

    public static final class StrVariant implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("repeat_value")
                .description("Repeat string values for N rows")
                .constArg("count", Schemas.INT64)
                .arg(new ArgSpec("values", 1, Schemas.UTF8, "", true, false, "",
                        List.of(), /*varargs=*/true, /*anyType=*/false))
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) {
            int n = Math.max(0, p.arguments().positional().size() - 1);
            return BindResponse.forSchema(schemaIpc(n, Schemas.UTF8));
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long count = ex.positional(0, "count").asLong().required();
            List<Object> tail = ex.varargsFrom(1);
            String[] values = new String[tail.size()];
            for (int i = 0; i < tail.size(); i++) {
                values[i] = (String) tail.get(i);
            }
            return new StrState((int) count, values, schema(tail.size(), Schemas.UTF8));
        }
    }

    /**
     * Shared shape: an N-row, M-column emitter where every cell in column
     * {@code v<c>} carries the same value. Subclasses parameterize the
     * per-column fill via {@link #fillColumn}.
     */
    public abstract static class RepeatState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int remaining;
        public Schema schema;

        protected RepeatState() {}
        RepeatState(int remaining, Schema schema) {
            this.remaining = remaining; this.schema = schema;
        }

        /** Fill column {@code v<c>} with the same value for {@code rows} rows. */
        protected abstract void fillColumn(org.apache.arrow.vector.VectorSchemaRoot root,
                                              int columnIndex, int rows);

        protected abstract int columnCount();

        @Override public final void produceTick(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) { out.finish(); return; }
            int n = Math.min(remaining, 1024);
            BatchUtil.emit(schema, n, out, (root, rows, start) -> {
                for (int c = 0; c < columnCount(); c++) fillColumn(root, c, rows);
            });
            remaining -= n;
        }
    }

    public static final class IntState extends RepeatState {
        private static final long serialVersionUID = 1L;
        public long[] values;

        public IntState() {}
        IntState(int remaining, long[] values, Schema schema) {
            super(remaining, schema);
            this.values = values;
        }

        @Override protected int columnCount() { return values.length; }
        @Override protected void fillColumn(org.apache.arrow.vector.VectorSchemaRoot root,
                                               int c, int rows) {
            BigIntVector v = (BigIntVector) root.getVector("v" + c);
            long val = values[c];
            for (int i = 0; i < rows; i++) v.setSafe(i, val);
        }
    }

    public static final class StrState extends RepeatState {
        private static final long serialVersionUID = 1L;
        public String[] values;

        public StrState() {}
        StrState(int remaining, String[] values, Schema schema) {
            super(remaining, schema);
            this.values = values;
        }

        @Override protected int columnCount() { return values.length; }
        @Override protected void fillColumn(org.apache.arrow.vector.VectorSchemaRoot root,
                                               int c, int rows) {
            VarCharVector v = (VarCharVector) root.getVector("v" + c);
            Text t = new Text(values[c] == null ? "" : values[c]);
            for (int i = 0; i < rows; i++) v.setSafe(i, t);
        }
    }
}
