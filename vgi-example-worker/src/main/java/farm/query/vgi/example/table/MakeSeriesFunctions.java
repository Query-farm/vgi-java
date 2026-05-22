// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.ParameterExtractor;
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
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Five overloads of {@code make_series}, dispatched by argCount and arg type:
 * <ul>
 *   <li>{@code (count BIGINT)} → 0..count-1</li>
 *   <li>{@code (start BIGINT, stop BIGINT)} → start..stop-1</li>
 *   <li>{@code (start BIGINT, stop BIGINT, step BIGINT)} → start..stop-1 step step</li>
 *   <li>{@code (csv VARCHAR)} → parsed comma-separated integers</li>
 *   <li>{@code (step DOUBLE)} → 10 floats: 0, step, 2*step, ..., 9*step</li>
 * </ul>
 */
public final class MakeSeriesFunctions {

    private static final Schema INT_SCHEMA = new Schema(List.of(
            Schemas.nullable("value", Schemas.INT64)));
    private static final byte[] INT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(INT_SCHEMA);

    private static final Schema FLOAT_SCHEMA = new Schema(List.of(
            Schemas.nullable("value", Schemas.FLOAT64)));
    private static final byte[] FLOAT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(FLOAT_SCHEMA);

    private MakeSeriesFunctions() {}

    public static final class IntState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public long[] values;
        public int offset;
        public boolean isFloat;

        public IntState() {}

        IntState(long[] values) { this.values = values; this.offset = 0; this.isFloat = false; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (offset >= values.length) { out.finish(); return; }
            int n = Math.min(1024, values.length - offset);
            VectorSchemaRoot root = VectorSchemaRoot.create(INT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("value");
            for (int i = 0; i < n; i++) v.setSafe(i, values[offset + i]);
            root.setRowCount(n);
            out.emit(root);
            offset += n;
        }
    }

    public static final class FloatState extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public double[] values;
        public int offset;

        public FloatState() {}

        FloatState(double[] values) { this.values = values; this.offset = 0; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (offset >= values.length) { out.finish(); return; }
            int n = Math.min(1024, values.length - offset);
            VectorSchemaRoot root = VectorSchemaRoot.create(FLOAT_SCHEMA, Allocators.root());
            root.allocateNew();
            Float8Vector v = (Float8Vector) root.getVector("value");
            for (int i = 0; i < n; i++) v.setSafe(i, values[offset + i]);
            root.setRowCount(n);
            out.emit(root);
            offset += n;
        }
    }

    /** {@code make_series(count BIGINT)} — 0..count-1. */
    public static final class Count implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_series")
                .description("Generate integers from 0 to count-1")
                .constArg("count", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            long count = ParameterExtractor.of(p.arguments())
                    .positional(0, "count").asLong().required();
            long[] values = new long[(int) Math.max(0, count)];
            for (int i = 0; i < values.length; i++) values[i] = i;
            return new IntState(values);
        }
        @Override
        public List<farm.query.vgi.catalog.ColumnStatistics> statistics(TableBindParams p) {
            Object countObj = p.arguments().positional().isEmpty()
                    ? null : p.arguments().positionalAt(0);
            if (!(countObj instanceof Number cn)) return null;
            long count = cn.longValue();
            if (count <= 0) return null;
            return List.of(farm.query.vgi.catalog.ColumnStatistics.ofInt64(
                    "value", 0L, count - 1, false, count));
        }
    }

    /** {@code make_series(start BIGINT, stop BIGINT)} — start..stop-1. */
    public static final class Range implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_series")
                .description("Generate integers from start to stop-1")
                .constArg("start", Schemas.INT64)
                .constArg("stop", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long start = ex.positional(0, "start").asLong().required();
            long stop = ex.positional(1, "stop").asLong().required();
            int n = (int) Math.max(0, stop - start);
            long[] values = new long[n];
            for (int i = 0; i < n; i++) values[i] = start + i;
            return new IntState(values);
        }
    }

    /** {@code make_series(start BIGINT, stop BIGINT, step BIGINT)} — with step. */
    public static final class Step implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_series")
                .description("Generate integers from start to stop-1 with step")
                .constArg("start", Schemas.INT64)
                .constArg("stop", Schemas.INT64)
                .constArg("step", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            ParameterExtractor ex = ParameterExtractor.of(p.arguments());
            long start = ex.positional(0, "start").asLong().required();
            long stop = ex.positional(1, "stop").asLong().required();
            long step = ex.positional(2, "step").asLong().ge(1).required();
            List<Long> tmp = new ArrayList<>();
            for (long v = start; v < stop; v += step) tmp.add(v);
            long[] values = new long[tmp.size()];
            for (int i = 0; i < values.length; i++) values[i] = tmp.get(i);
            return new IntState(values);
        }
    }

    /** {@code make_series(csv VARCHAR)} — parsed comma-separated integers. */
    public static final class Csv implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_series")
                .description("Parse comma-separated integers into rows")
                .constArg("values", Schemas.UTF8)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            String csv = ParameterExtractor.of(p.arguments())
                    .positional(0, "values").asString().required();
            String[] parts = csv.split(",");
            long[] values = new long[parts.length];
            for (int i = 0; i < parts.length; i++) values[i] = Long.parseLong(parts[i].trim());
            return new IntState(values);
        }
    }

    /** {@code make_series(step DOUBLE)} — 10 floats: 0, step, 2*step, ..., 9*step. */
    public static final class FloatStep implements TableFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("make_series")
                .description("Generate 10 float values with given step size")
                .constArg("step", Schemas.FLOAT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(FLOAT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            double step = ParameterExtractor.of(p.arguments())
                    .positional(0, "step").asDouble().required();
            double[] values = new double[10];
            for (int i = 0; i < 10; i++) values[i] = i * step;
            return new FloatState(values);
        }
    }
}
