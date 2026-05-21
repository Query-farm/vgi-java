// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.math.BigInteger;
import java.util.List;

/**
 * {@code ordered_source} — emits a fixed 0..15 sequence via
 * {@code source_order_dependent=true}. Input is ignored; {@code combine()}
 * returns 16 ascending 4-byte big-endian finalize_state_ids, and each finalize
 * stream emits one row holding that integer. With FIXED_ORDER the C++ Source
 * must yield rows in 0..15 order. Mirrors vgi-python {@code OrderedSourceFunction}.
 */
public final class OrderedSourceFunction implements TableBufferingFunction {

    private static final int N_ROWS = 16;
    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("v", Schemas.INT64));

    @Override public String name() { return "ordered_source"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(
                "Emits a fixed 0..15 sequence via source_order_dependent=True; input is ignored")
                .withCategories("test", "ordering");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.table("data", 0));
    }

    @Override public boolean sourceOrderDependent() { return true; }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
    }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        return params.executionId();  // input ignored
    }

    @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        java.util.List<byte[]> ids = new java.util.ArrayList<>(N_ROWS);
        for (int i = 0; i < N_ROWS; i++) {
            ids.add(new byte[]{
                    (byte) (i >>> 24), (byte) (i >>> 16), (byte) (i >>> 8), (byte) i});
        }
        return ids;
    }

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new OneShotProducer(params);
    }

    /** Emits exactly one row carrying the integer decoded from finalize_state_id. */
    private static final class OneShotProducer extends BufferingFinalizeProducer {
        private final long value;
        private boolean emitted = false;

        OneShotProducer(TableBufferingFinalizeParams params) {
            super(params);
            this.value = new BigInteger(1, params.finalizeStateId()).longValue();
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted) { out.finish(); return; }
            Schema schema = outputSchema != null ? outputSchema : OUTPUT;
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector("v")).setSafe(0, value);
            root.setRowCount(1);
            out.emit(root);
            emitted = true;
        }
    }
}
