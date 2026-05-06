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
 * {@code echo(data TABLE) -> *} — passes each input batch through unchanged.
 * Output schema = input schema.
 */
public final class EchoFunction implements TableInOutFunction {

    @Override public String name() { return "echo"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Passthrough function that emits each input batch unchanged")
                .withCategories("utility", "debug");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.table("data", 0));
    }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            // Catalog enumeration with no input — placeholder empty schema.
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(in));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new EchoState();
    }

    public static final class EchoState extends TableInOutExchangeState {
        public EchoState() {}
        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            // Copy the input batch into a fresh root so the framework's
            // wire writer never observes a mutating source. With nested
            // types (struct/list/map) DuckDB streams multiple ticks per
            // call; sharing the source root truncated everything past the
            // first 2048-row vector.
            org.apache.arrow.vector.VectorSchemaRoot src = input.root();
            org.apache.arrow.vector.VectorSchemaRoot dst =
                    org.apache.arrow.vector.VectorSchemaRoot.create(
                            src.getSchema(), farm.query.vgirpc.wire.Allocators.root());
            dst.allocateNew();
            for (int c = 0; c < src.getFieldVectors().size(); c++) {
                org.apache.arrow.vector.FieldVector srcV = src.getFieldVectors().get(c);
                org.apache.arrow.vector.FieldVector dstV = dst.getFieldVectors().get(c);
                org.apache.arrow.vector.util.TransferPair tp = srcV.makeTransferPair(dstV);
                tp.splitAndTransfer(0, src.getRowCount());
            }
            dst.setRowCount(src.getRowCount());
            out.emit(dst);
        }
    }
}
