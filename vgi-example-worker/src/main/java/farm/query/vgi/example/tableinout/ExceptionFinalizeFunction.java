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
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * {@code exception_finalize(data TABLE)} — accumulates input then raises an
 * intentional exception during FINALIZE. Exercises the FINALIZE-phase init
 * path + error propagation.
 */
public final class ExceptionFinalizeFunction implements TableInOutFunction {

    @Override public String name() { return "exception_finalize"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Test function that raises exception during finalize phase");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.table("data", 0));
    }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(in));
    }

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
