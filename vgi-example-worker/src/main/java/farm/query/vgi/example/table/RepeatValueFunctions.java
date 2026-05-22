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
            long count = ((Number) p.arguments().positionalAt(0)).longValue();
            int n = p.arguments().positional().size() - 1;
            long[] values = new long[n];
            for (int i = 0; i < n; i++) {
                values[i] = ((Number) p.arguments().positionalAt(i + 1)).longValue();
            }
            return new IntState((int) count, values, schema(n, Schemas.INT64));
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
            long count = ((Number) p.arguments().positionalAt(0)).longValue();
            int n = p.arguments().positional().size() - 1;
            String[] values = new String[n];
            for (int i = 0; i < n; i++) {
                values[i] = (String) p.arguments().positionalAt(i + 1);
            }
            return new StrState((int) count, values, schema(n, Schemas.UTF8));
        }
    }

    public static final class IntState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int remaining;
        public long[] values;
        public Schema schema;

        public IntState() {}

        IntState(int remaining, long[] values, Schema schema) {
            this.remaining = remaining; this.values = values; this.schema = schema;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) { out.finish(); return; }
            int n = Math.min(remaining, 1024);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            for (int c = 0; c < values.length; c++) {
                BigIntVector v = (BigIntVector) root.getVector("v" + c);
                long val = values[c];
                for (int i = 0; i < n; i++) v.setSafe(i, val);
            }
            root.setRowCount(n);
            out.emit(root);
            remaining -= n;
        }
    }

    public static final class StrState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public int remaining;
        public String[] values;
        public Schema schema;

        public StrState() {}

        StrState(int remaining, String[] values, Schema schema) {
            this.remaining = remaining; this.values = values; this.schema = schema;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (remaining <= 0) { out.finish(); return; }
            int n = Math.min(remaining, 1024);
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            for (int c = 0; c < values.length; c++) {
                VarCharVector v = (VarCharVector) root.getVector("v" + c);
                Text t = new Text(values[c] == null ? "" : values[c]);
                for (int i = 0; i < n; i++) v.setSafe(i, t);
            }
            root.setRowCount(n);
            out.emit(root);
            remaining -= n;
        }
    }
}
