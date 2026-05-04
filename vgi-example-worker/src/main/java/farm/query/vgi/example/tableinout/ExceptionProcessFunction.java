// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;

import java.util.HashMap;
import java.util.Map;

/**
 * Throws an exception on every other input batch during process. Inherits the
 * sum-accumulation behaviour for non-failing batches so the test fixture works
 * even up to the point of failure.
 */
public final class ExceptionProcessFunction extends SumAllColumnsFunction {

    @Override public String name() { return "exception_process"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Test function that raises exception on second batch during process");
    }

    @Override
    public java.util.List<org.apache.arrow.vector.VectorSchemaRoot> finalizeBatches(
            TableInOutExchangeState state, TableInOutInitParams params) {
        // Match vgi-python/vgi-go: ignore accumulated sums (the test contract
        // is "empty storage produces a clean zero-sum row").
        SumAllColumnsFunction.SumState s = (SumAllColumnsFunction.SumState) state;
        Map<String, Long> zeroInts = new HashMap<>();
        for (String k : s.intSums.keySet()) zeroInts.put(k, 0L);
        Map<String, Double> zeroFloats = new HashMap<>();
        for (String k : s.floatSums.keySet()) zeroFloats.put(k, 0.0);
        SumAllColumnsFunction.SumState zero =
                new SumAllColumnsFunction.SumState(zeroInts, zeroFloats, s.outputSchemaIpc);
        return super.finalizeBatches(zero, params);
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        Map<String, Long> intSums = new HashMap<>();
        Map<String, Double> floatSums = new HashMap<>();
        for (org.apache.arrow.vector.types.pojo.Field f : params.outputSchema().getFields()) {
            if (f.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int) {
                intSums.put(f.getName(), 0L);
            } else if (f.getType() instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) {
                floatSums.put(f.getName(), 0.0);
            }
        }
        return new ThrowingState(intSums, floatSums, SchemaUtil.serializeSchema(params.outputSchema()));
    }

    public static final class ThrowingState extends SumAllColumnsFunction.SumState {
        public int batchCount;

        public ThrowingState() {}

        ThrowingState(Map<String, Long> intSums, Map<String, Double> floatSums, byte[] outputSchemaIpc) {
            super(intSums, floatSums, outputSchemaIpc);
        }

        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            batchCount++;
            if (batchCount % 2 == 0) {
                throw new RuntimeException("Intentional exception on batch " + batchCount);
            }
            super.onInputBatch(input, out, ctx);
        }
    }
}
