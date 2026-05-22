// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/**
 * {@code echo_witness(data TABLE) -> *} — projection-pushdown instrumentation.
 *
 * <p>Output schema equals the input schema, but every emitted cell carries the
 * integer count of columns in the (post-projection-narrowing) output schema the
 * worker actually observed. {@code SELECT a FROM echo_witness(3-col)} yields
 * {@code 1} when projection reached the worker; {@code 3} if it did not. Mirrors
 * vgi-python {@code EchoWitnessFunction}.
 */
public final class EchoWitnessFunction extends PassthroughTIOFunction {

    // projection pushdown ON, filter pushdown OFF — probes projection only.
    private static final FunctionSpec SPEC = FunctionSpec.builder("echo_witness")
            .metadata(FunctionMetadata.describe("Emits len(observed_output_schema) per column — projection probe")
                    .withPushdown(true, false, false)
                    .withCategories("debug"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new WitnessState(params.outputSchema());
    }

    private static final class WitnessState extends TableInOutExchangeState {
        private final Schema outputSchema;

        WitnessState(Schema outputSchema) { this.outputSchema = outputSchema; }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            int rows = input.root().getRowCount();
            int witness = outputSchema.getFields().size();
            List<FieldVector> vectors = new ArrayList<>(witness);
            for (var field : outputSchema.getFields()) {
                FieldVector v = field.createVector(Allocators.root());
                v.allocateNew();
                if (v instanceof IntVector iv) {
                    for (int r = 0; r < rows; r++) iv.setSafe(r, witness);
                } else if (v instanceof BigIntVector bv) {
                    for (int r = 0; r < rows; r++) bv.setSafe(r, witness);
                } else {
                    throw new IllegalStateException(
                            "echo_witness only supports integer columns, got " + field.getType());
                }
                v.setValueCount(rows);
                vectors.add(v);
            }
            VectorSchemaRoot outRoot = new VectorSchemaRoot(vectors);
            outRoot.setRowCount(rows);
            out.emit(outRoot);
        }
    }
}
