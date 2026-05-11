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
            farm.query.vgi.internal.SchemaUtil.serializeSchema(INT_SCHEMA);

    private static final Schema FLOAT_SCHEMA = new Schema(List.of(
            Schemas.nullable("value", Schemas.FLOAT64)));
    private static final byte[] FLOAT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(FLOAT_SCHEMA);

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
        @Override public String name() { return "make_series"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Generate integers from 0 to count-1"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("count", 0, Schemas.INT64, true));
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            long count = ((Number) p.arguments().positionalAt(0)).longValue();
            long[] values = new long[(int) Math.max(0, count)];
            for (int i = 0; i < values.length; i++) values[i] = i;
            return new IntState(values);
        }
    }

    /** {@code make_series(start BIGINT, stop BIGINT)} — start..stop-1. */
    public static final class Range implements TableFunction {
        @Override public String name() { return "make_series"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Generate integers from start to stop-1"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    new ArgSpec("start", 0, Schemas.INT64, true),
                    new ArgSpec("stop", 1, Schemas.INT64, true));
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            long start = ((Number) p.arguments().positionalAt(0)).longValue();
            long stop = ((Number) p.arguments().positionalAt(1)).longValue();
            int n = (int) Math.max(0, stop - start);
            long[] values = new long[n];
            for (int i = 0; i < n; i++) values[i] = start + i;
            return new IntState(values);
        }
    }

    /** {@code make_series(start BIGINT, stop BIGINT, step BIGINT)} — with step. */
    public static final class Step implements TableFunction {
        @Override public String name() { return "make_series"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Generate integers from start to stop-1 with step"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(
                    new ArgSpec("start", 0, Schemas.INT64, true),
                    new ArgSpec("stop", 1, Schemas.INT64, true),
                    new ArgSpec("step", 2, Schemas.INT64, true));
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            long start = ((Number) p.arguments().positionalAt(0)).longValue();
            long stop = ((Number) p.arguments().positionalAt(1)).longValue();
            long step = ((Number) p.arguments().positionalAt(2)).longValue();
            if (step < 1) throw new IllegalArgumentException("step must be >= 1, got " + step);
            List<Long> tmp = new ArrayList<>();
            for (long v = start; v < stop; v += step) tmp.add(v);
            long[] values = new long[tmp.size()];
            for (int i = 0; i < values.length; i++) values[i] = tmp.get(i);
            return new IntState(values);
        }
    }

    /** {@code make_series(csv VARCHAR)} — parsed comma-separated integers. */
    public static final class Csv implements TableFunction {
        @Override public String name() { return "make_series"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Parse comma-separated integers into rows"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("values", 0, Schemas.UTF8, true));
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(INT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            String csv = (String) p.arguments().positionalAt(0);
            String[] parts = csv.split(",");
            long[] values = new long[parts.length];
            for (int i = 0; i < parts.length; i++) values[i] = Long.parseLong(parts[i].trim());
            return new IntState(values);
        }
    }

    /** {@code make_series(step DOUBLE)} — 10 floats: 0, step, 2*step, ..., 9*step. */
    public static final class FloatStep implements TableFunction {
        @Override public String name() { return "make_series"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("Generate 10 float values with given step size"); }
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(new ArgSpec("step", 0, Schemas.FLOAT64, true));
        }
        @Override public BindResponse onBind(TableBindParams p) { return BindResponse.forSchema(FLOAT_SCHEMA_IPC); }
        @Override public TableProducerState createProducer(TableInitParams p) {
            double step = ((Number) p.arguments().positionalAt(0)).doubleValue();
            double[] values = new double[10];
            for (int i = 0; i < 10; i++) values[i] = i * step;
            return new FloatState(values);
        }
    }
}
