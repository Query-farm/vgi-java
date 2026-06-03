// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;

import java.util.List;

/**
 * {@code echo(data TABLE) -> *} — passes each input batch through unchanged.
 * Output schema = input schema.
 */
public final class EchoFunction extends PassthroughTIOFunction {

    @Override public String name() { return "echo"; }

    @Override public FunctionMetadata metadata() {
        // Projection pushdown: DuckDB sends projection_ids, the framework
        // narrows the output schema, and we emit only the requested columns
        // (no narrowing PROJECTION node above the operator). Filter pushdown
        // stays off — the InOut path always runs filters via a FILTER node.
        return FunctionMetadata.describe("Passthrough function that emits each input batch unchanged")
                .withPushdown(true, false, false)
                .withCategories("utility", "debug");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.table("data", 0));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new EchoState(params.outputSchema());
    }

    public static final class EchoState extends TableInOutExchangeState {
        private final org.apache.arrow.vector.types.pojo.Schema outputSchema;
        public EchoState() { this.outputSchema = null; }
        public EchoState(org.apache.arrow.vector.types.pojo.Schema outputSchema) {
            this.outputSchema = outputSchema;
        }
        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            // Transfer the input batch's vectors into a fresh VSR so the
            // framework can close() the emitted root after writing without
            // releasing the inputReader's buffers. Naive emit(input.root())
            // works for one tick but invalidates the reader on the second
            // tick — observed as multi-batch truncation in nested-type
            // round-trips. TransferPair preserves dict-encoded children
            // because the dictionary provider lives on the IpcStreamReader
            // (passed through by flushCollector), not on the vectors.
            org.apache.arrow.vector.VectorSchemaRoot in = input.root();
            int rows = in.getRowCount();
            java.util.List<org.apache.arrow.vector.FieldVector> outVectors = new java.util.ArrayList<>();
            // Select the (already projection-narrowed) output columns by name;
            // falls back to all input columns when no schema was captured.
            java.util.List<org.apache.arrow.vector.FieldVector> sources;
            if (outputSchema == null) {
                sources = in.getFieldVectors();
            } else {
                sources = new java.util.ArrayList<>();
                for (org.apache.arrow.vector.types.pojo.Field f : outputSchema.getFields()) {
                    sources.add((org.apache.arrow.vector.FieldVector) in.getVector(f.getName()));
                }
            }
            for (org.apache.arrow.vector.FieldVector v : sources) {
                org.apache.arrow.vector.util.TransferPair tp = v.getTransferPair(farm.query.vgirpc.wire.Allocators.root());
                tp.transfer();
                outVectors.add((org.apache.arrow.vector.FieldVector) tp.getTo());
            }
            org.apache.arrow.vector.VectorSchemaRoot copy =
                    new org.apache.arrow.vector.VectorSchemaRoot(outVectors);
            copy.setRowCount(rows);
            out.emit(copy);
        }
    }
}
