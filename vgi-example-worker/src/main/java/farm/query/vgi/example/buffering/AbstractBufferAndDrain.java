// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Base for "buffer every input batch, emit one per finalize tick" buffering
 * functions. Mirrors vgi-python {@code BufferInputFunction}: {@code process()}
 * appends the batch's IPC bytes to the {@code "buf"} log; {@code combine()}
 * collapses to a single finalize stream keyed by {@code execution_id};
 * {@code createFinalizeProducer()} cursor-drains the log one batch per tick.
 */
public abstract class AbstractBufferAndDrain implements TableBufferingFunction {

    static final byte[] NS_BUF = "buf".getBytes(StandardCharsets.UTF_8);
    static final byte[] KEY = new byte[0];

    /** Passthrough: output schema = input schema. */
    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        if (in == null || in.getFields().isEmpty()) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        return BindResponse.forSchema(SchemaUtil.serializeSchema(in));
    }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        params.storage().stateAppend(NS_BUF, KEY, BatchUtil.writeSingleBatch(batch));
        return params.executionId();
    }

    @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        return List.of(params.executionId());
    }

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new LogDrainProducer(params, NS_BUF);
    }

    /**
     * Drains a {@code (ns, "")} state-log, one buffered batch per tick. The log
     * is read {@value #PAGE} entries per storage call (one round trip on a
     * remote backend serves several ticks); the page is process-local and
     * transient, so after an HTTP state-token round trip the producer re-reads
     * from {@code afterId}, the last entry it actually emitted.
     */
    static final class LogDrainProducer extends BufferingFinalizeProducer {
        static final int PAGE = 16;
        private byte[] ns;
        private long afterId = -1;
        private transient java.util.ArrayDeque<FunctionStorage.LogEntry> page;
        private transient boolean exhausted;

        LogDrainProducer() {}

        LogDrainProducer(TableBufferingFinalizeParams params, byte[] ns) {
            super(params);
            this.ns = ns;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (page == null) page = new java.util.ArrayDeque<>();
            if (page.isEmpty() && !exhausted) {
                List<FunctionStorage.LogEntry> rows = storage().stateLogScan(ns, KEY, afterId, PAGE);
                page.addAll(rows);
                // A short page means the log is drained; skip the empty re-read.
                exhausted = rows.size() < PAGE;
            }
            FunctionStorage.LogEntry e = page.poll();
            if (e == null) {
                out.finish();
                return;
            }
            VectorSchemaRoot full = BatchUtil.readSingleBatch(e.value(), Allocators.root());
            emitProjected(full, out);
            full.close();
            afterId = e.id();
        }
    }
}
