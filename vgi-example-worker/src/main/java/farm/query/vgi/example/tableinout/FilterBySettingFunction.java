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
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.util.List;
import java.util.Map;

/**
 * {@code filter_by_setting(data TABLE)} — passes through input rows whose
 * {@code value} column is {@code >=} the {@code threshold} DuckDB setting.
 */
public final class FilterBySettingFunction implements TableInOutFunction {

    @Override public String name() { return "filter_by_setting"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Filter rows where value column >= threshold setting");
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
        long threshold = settingsToLong(params.settings(), "threshold", 0L);
        return new State(threshold, SchemaUtil.serializeSchema(params.inputSchema()));
    }

    private static long settingsToLong(Map<String, Object> settings, String name, long fallback) {
        Object v = settings == null ? null : settings.get(name);
        if (v instanceof Number n) return n.longValue();
        if (v instanceof CharSequence cs) {
            try { return Long.parseLong(cs.toString()); } catch (NumberFormatException ignore) {}
        }
        return fallback;
    }

    public static final class State extends TableInOutExchangeState {
        public long threshold;
        public byte[] schemaIpc;

        public State() {}
        State(long threshold, byte[] schemaIpc) { this.threshold = threshold; this.schemaIpc = schemaIpc; }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot src = input.root();
            FieldVector value = src.getVector("value");
            int rows = src.getRowCount();
            int[] keep = new int[rows];
            int kept = 0;
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i)) continue;
                long v = farm.query.vgi.types.ScalarHelpers.toLong(value, i);
                if (v >= threshold) keep[kept++] = i;
            }
            Schema schema = SchemaUtil.deserializeSchema(schemaIpc);
            VectorSchemaRoot dst = VectorSchemaRoot.create(schema, Allocators.root());
            dst.allocateNew();
            for (int c = 0; c < src.getFieldVectors().size(); c++) {
                FieldVector srcV = src.getFieldVectors().get(c);
                FieldVector dstV = dst.getFieldVectors().get(c);
                TransferPair tp = srcV.makeTransferPair(dstV);
                for (int r = 0; r < kept; r++) tp.copyValueSafe(keep[r], r);
            }
            dst.setRowCount(kept);
            out.emit(dst);
        }
    }
}
