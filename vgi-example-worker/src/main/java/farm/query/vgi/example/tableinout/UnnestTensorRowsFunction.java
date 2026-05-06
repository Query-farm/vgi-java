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
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Stub for {@code unnest_tensor_rows(data TABLE)} — emits input passthrough.
 * Required for function_registration coverage; tensor unnesting semantics
 * are not yet implemented.
 */
public final class UnnestTensorRowsFunction implements TableInOutFunction {

    @Override public String name() { return "unnest_tensor_rows"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Unnest tensor rows (stub)");
    }
    @Override public List<ArgSpec> argumentSpecs() { return List.of(ArgSpec.table("data", 0)); }

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

    public static final class State extends TableInOutExchangeState {
        public State() {}
        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            out.emit(input.root());
        }
    }
}
