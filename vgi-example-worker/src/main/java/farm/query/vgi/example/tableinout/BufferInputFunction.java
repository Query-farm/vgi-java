// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
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

import java.util.ArrayList;
import java.util.List;

/**
 * {@code buffer_input(data TABLE) -> *} — buffers every input batch in
 * exchange state, emits empty batches during INPUT, then drains the buffer
 * during FINALIZE. Used by table_in_out/buffer_input/* tests.
 */
public final class BufferInputFunction extends PassthroughTIOFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("buffer_input")
            .description("Collects all input batches and emits during finalization")
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new BufferState(SchemaUtil.serializeSchema(params.inputSchema()));
    }

    @Override public boolean hasFinalize() { return true; }

    @Override
    public List<VectorSchemaRoot> finalizeBatches(TableInOutExchangeState state, TableInOutInitParams params) {
        BufferState s = (BufferState) state;
        Schema schema = SchemaUtil.deserializeSchema(s.schemaIpc);
        List<VectorSchemaRoot> out = new ArrayList<>();
        for (byte[] bytes : s.batches) {
            try (var bais = new java.io.ByteArrayInputStream(bytes);
                 var reader = new org.apache.arrow.vector.ipc.ArrowStreamReader(bais, Allocators.root())) {
                while (reader.loadNextBatch()) {
                    VectorSchemaRoot src = reader.getVectorSchemaRoot();
                    VectorSchemaRoot dst = VectorSchemaRoot.create(schema, Allocators.root());
                    dst.allocateNew();
                    for (int i = 0; i < src.getFieldVectors().size(); i++) {
                        FieldVector srcV = src.getFieldVectors().get(i);
                        FieldVector dstV = dst.getFieldVectors().get(i);
                        TransferPair tp = srcV.makeTransferPair(dstV);
                        tp.transfer();
                    }
                    dst.setRowCount(src.getRowCount());
                    out.add(dst);
                }
            } catch (Exception e) {
                throw new RuntimeException("buffer_input finalize replay", e);
            }
        }
        s.batches.clear();
        return out;
    }

    public static final class BufferState extends TableInOutExchangeState {
        public byte[] schemaIpc;
        public List<byte[]> batches = new ArrayList<>();

        public BufferState() {}
        BufferState(byte[] schemaIpc) { this.schemaIpc = schemaIpc; }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            // Serialise the input batch into IPC bytes so it survives the
            // INPUT→FINALIZE phase boundary even if the source root is freed.
            try (var baos = new java.io.ByteArrayOutputStream();
                 var writer = new org.apache.arrow.vector.ipc.ArrowStreamWriter(
                         input.root(), null, java.nio.channels.Channels.newChannel(baos))) {
                writer.start();
                writer.writeBatch();
                writer.end();
                batches.add(baos.toByteArray());
            } catch (Exception e) {
                throw new RuntimeException("buffer_input onInputBatch", e);
            }
            // INPUT phase still demands an output batch each tick (even empty).
            VectorSchemaRoot empty = VectorSchemaRoot.create(
                    SchemaUtil.deserializeSchema(schemaIpc), Allocators.root());
            empty.setRowCount(0);
            out.emit(empty);
        }
    }
}
