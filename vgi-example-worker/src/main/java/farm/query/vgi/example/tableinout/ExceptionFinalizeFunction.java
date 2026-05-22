// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.List;

/**
 * {@code exception_finalize(data TABLE)} — accumulates input then raises an
 * intentional exception during FINALIZE. Exercises the FINALIZE-phase init
 * path + error propagation.
 */
public final class ExceptionFinalizeFunction extends PassthroughTIOFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("exception_finalize")
            .description("Test function that raises exception during finalize")
            .table("data")
            .named("logging", farm.query.vgi.types.Schemas.BOOL, "false")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new State();
    }

    @Override public boolean hasFinalize() { return true; }

    @Override
    public List<VectorSchemaRoot> finalizeBatches(TableInOutExchangeState state, TableInOutInitParams params) {
        throw new RuntimeException("Intentional exception during finalize()");
    }

    public static final class State extends TableInOutExchangeState {
        public State() {}
        @Override
        public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            // Drop input rows; emit an empty batch matching the output schema.
            // OutputCollector requires exactly one data emit per call for exchange.
            VectorSchemaRoot src = input.root();
            try (VectorSchemaRoot empty = VectorSchemaRoot.create(src.getSchema(),
                    farm.query.vgirpc.wire.Allocators.root())) {
                empty.allocateNew();
                empty.setRowCount(0);
                out.emit(empty);
            }
        }
    }
}
