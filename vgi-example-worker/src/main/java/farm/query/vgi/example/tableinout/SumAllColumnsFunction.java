// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@code sum_all_columns(data TABLE)} — accumulates column-wise sums across
 * all input batches, emitting one row of totals at FINALIZE. Numeric input
 * columns become the output schema (ints widen to int64, floats to float64).
 */
public class SumAllColumnsFunction implements TableInOutFunction {

    @Override public String name() { return "sum_all_columns"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Computes column-wise sums across all batches")
                .withCategories("aggregation", "numeric");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.table("data", 0),
                new ArgSpec("logging", -1, Schemas.BOOL, "", true, true, "false",
                        List.of(), false, false));
    }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        List<Field> outFields = new ArrayList<>();
        for (Field f : in.getFields()) {
            ArrowType t = f.getType();
            if (t instanceof ArrowType.Int) {
                outFields.add(new Field(f.getName(), new FieldType(true, Schemas.INT64, null), null));
            } else if (t instanceof ArrowType.FloatingPoint) {
                outFields.add(new Field(f.getName(), new FieldType(true, Schemas.FLOAT64, null), null));
            }
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(outFields)));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        Map<String, Long> intSums = new HashMap<>();
        Map<String, Double> floatSums = new HashMap<>();
        for (Field f : params.outputSchema().getFields()) {
            if (f.getType() instanceof ArrowType.Int) intSums.put(f.getName(), 0L);
            else if (f.getType() instanceof ArrowType.FloatingPoint) floatSums.put(f.getName(), 0.0);
        }
        Object loggingObj = params.arguments().named().get("logging");
        boolean logging = loggingObj instanceof Boolean b && b;
        return new SumState(intSums, floatSums, SchemaUtil.serializeSchema(params.outputSchema()), logging);
    }

    @Override public boolean hasFinalize() { return true; }

    @Override
    public List<VectorSchemaRoot> finalizeBatches(TableInOutExchangeState state, TableInOutInitParams params) {
        SumState s = (SumState) state;
        Schema schema = s.schema();
        VectorSchemaRoot out = VectorSchemaRoot.create(schema, Allocators.root());
        out.allocateNew();
        for (Field f : schema.getFields()) {
            FieldVector v = out.getVector(f.getName());
            if (s.intSums.containsKey(f.getName())) {
                ((BigIntVector) v).setSafe(0, s.intSums.get(f.getName()));
            } else if (s.floatSums.containsKey(f.getName())) {
                ((Float8Vector) v).setSafe(0, s.floatSums.get(f.getName()));
            }
        }
        out.setRowCount(1);
        return List.of(out);
    }

    /** TIO state — exposed as a static class so subclasses (e.g. exception_process) can extend behaviour. */
    public static class SumState extends TableInOutExchangeState {
        public Map<String, Long> intSums;
        public Map<String, Double> floatSums;
        public byte[] outputSchemaIpc;
        public boolean logging;
        public long batchCount;

        private transient Schema cachedSchema;

        public SumState() {}

        public SumState(Map<String, Long> intSums, Map<String, Double> floatSums, byte[] outputSchemaIpc) {
            this(intSums, floatSums, outputSchemaIpc, false);
        }

        public SumState(Map<String, Long> intSums, Map<String, Double> floatSums, byte[] outputSchemaIpc,
                          boolean logging) {
            this.intSums = intSums;
            this.floatSums = floatSums;
            this.outputSchemaIpc = outputSchemaIpc;
            this.logging = logging;
        }

        public Schema schema() {
            if (cachedSchema == null) cachedSchema = SchemaUtil.deserializeSchema(outputSchemaIpc);
            return cachedSchema;
        }

        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            Schema schema = schema();
            VectorSchemaRoot src = input.root();
            int rows = src.getRowCount();
            if (logging) {
                batchCount++;
                out.clientLog(new farm.query.vgirpc.log.Message(
                        farm.query.vgirpc.log.Level.INFO,
                        "Processing batch with " + rows + " rows",
                        null));
            }
            for (Field f : schema.getFields()) {
                FieldVector col = src.getVector(f.getName());
                if (col == null) continue;
                if (intSums.containsKey(f.getName())) {
                    long sum = 0;
                    for (int i = 0; i < rows; i++) {
                        if (col.isNull(i)) continue;
                        sum += readLong(col, i);
                    }
                    intSums.merge(f.getName(), sum, Long::sum);
                } else if (floatSums.containsKey(f.getName())) {
                    double sum = 0;
                    for (int i = 0; i < rows; i++) {
                        if (col.isNull(i)) continue;
                        sum += readDouble(col, i);
                    }
                    floatSums.merge(f.getName(), sum, Double::sum);
                }
            }
            // Emit empty batch to satisfy the exchange protocol.
            try (VectorSchemaRoot empty = VectorSchemaRoot.create(schema, Allocators.root())) {
                empty.allocateNew();
                empty.setRowCount(0);
                out.emit(empty);
            }
        }

        private static long readLong(FieldVector v, int i) {
            if (v instanceof BigIntVector b) return b.get(i);
            if (v instanceof IntVector iv) return iv.get(i);
            if (v instanceof SmallIntVector s) return s.get(i);
            if (v instanceof TinyIntVector t) return t.get(i);
            return 0;
        }

        private static double readDouble(FieldVector v, int i) {
            if (v instanceof Float8Vector f) return f.get(i);
            if (v instanceof Float4Vector f) return f.get(i);
            if (v instanceof BigIntVector b) return b.get(i);
            if (v instanceof IntVector iv) return iv.get(i);
            return 0;
        }
    }
}
