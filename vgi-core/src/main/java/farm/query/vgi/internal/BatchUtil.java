// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.internal;

import farm.query.vgi.pushdown.FilterApplier;
import farm.query.vgi.table.BatchState;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.channels.Channels;

/** Helpers for round-tripping Arrow IPC record-batch byte blobs. */
public final class BatchUtil {

    private BatchUtil() {}

    /** Read a single batch out of an IPC stream blob. Caller owns the returned root. */
    public static VectorSchemaRoot readSingleBatch(byte[] data, BufferAllocator alloc) {
        try {
            ByteArrayInputStream in = new ByteArrayInputStream(data);
            ArrowStreamReader reader = new ArrowStreamReader(in, alloc);
            if (!reader.loadNextBatch()) {
                reader.close();
                return null;
            }
            // Detach the root so the caller can keep using it after closing the reader.
            // VectorSchemaRoot.slice retains the buffers via reference counting so the
            // returned slice survives the reader's close.
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            VectorSchemaRoot dst = src.slice(0, src.getRowCount());
            reader.close();
            return dst;
        } catch (Exception e) {
            throw new RuntimeException("BatchUtil.readSingleBatch failed", e);
        }
    }

    /** Run {@code body} against a freshly-loaded batch with the reader still open. */
    public static <R> R withReadBatch(byte[] data, BufferAllocator alloc, java.util.function.Function<VectorSchemaRoot, R> body) {
        try (ByteArrayInputStream in = new ByteArrayInputStream(data);
             ArrowStreamReader reader = new ArrowStreamReader(in, alloc)) {
            if (!reader.loadNextBatch()) return body.apply(null);
            return body.apply(reader.getVectorSchemaRoot());
        } catch (Exception e) {
            throw new RuntimeException("BatchUtil.withReadBatch failed", e);
        }
    }

    /**
     * Producer-tick helper: emit one batch from a {@link BatchState} cursor.
     *
     * <p>Encapsulates the universal "if done finish; allocate; fill;
     * (optionally) filter; emit; advance" pattern used by every count-based
     * table producer. The {@link ColumnFiller} only writes columns; the
     * helper handles allocator + row-count + filter application.
     *
     * @param batch  state cursor (mutated in place via {@code advance})
     * @param schema output schema used to allocate the batch root
     * @param filters optional pushdown filter applier; {@code null} for none
     * @param out    collector to emit into
     * @param filler caller's column-fill body; receives the freshly-allocated
     *               root, the row count for this batch, and the starting row
     *               index (so callers can compute {@code start + i} values).
     */
    public static void produceBatch(BatchState batch, Schema schema, FilterApplier filters,
                                      OutputCollector out, ColumnFiller filler) {
        if (batch.done()) { out.finish(); return; }
        int n = batch.nextBatchSize();
        long start = batch.index();
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        filler.fill(root, n, start);
        root.setRowCount(n);
        if (filters != null) root = filters.apply(root);
        out.emit(root);
        batch.advance(n);
    }

    /** User-supplied per-batch column-fill callback for {@link #produceBatch}. */
    @FunctionalInterface
    public interface ColumnFiller {
        void fill(VectorSchemaRoot root, int rowCount, long startRowIndex);
    }

    /**
     * Producer-tick helper for {@link org.apache.arrow.vector.ipc.ArrowReader}-
     * backed scans (JDBC-Arrow, Parquet, HTTP-Arrow streams).
     *
     * <p>On each call: load the next batch from {@code reader}, slice it so the
     * batch's buffers detach from the reader's lifecycle, optionally
     * {@linkplain VectorProjector#relabel relabel} to {@code target} when the
     * reader's schema differs only in column names, apply any pushdown
     * {@code filters}, and emit. Returns {@code true} when a batch was
     * emitted; {@code false} when the reader is exhausted (and {@code finish}
     * has been called on the collector). Producers typically close the reader
     * after a {@code false} return.
     *
     * @param reader  open Arrow reader; not closed by this helper
     * @param target  desired output schema for the emitted root, or
     *                {@code null} to emit the reader's native schema unchanged
     * @param filters optional pushdown filter applier; {@code null} for none
     * @param out     collector to emit into
     */
    public static boolean pumpArrowReader(org.apache.arrow.vector.ipc.ArrowReader reader,
                                            Schema target, FilterApplier filters,
                                            OutputCollector out) {
        try {
            if (!reader.loadNextBatch()) {
                out.finish();
                return false;
            }
            VectorSchemaRoot src = reader.getVectorSchemaRoot();
            VectorSchemaRoot detached = src.slice(0, src.getRowCount());
            VectorSchemaRoot dst = target == null ? detached : VectorProjector.relabel(detached, target);
            if (filters != null) dst = filters.apply(dst);
            out.emit(dst);
            return true;
        } catch (Exception e) {
            throw new RuntimeException("BatchUtil.pumpArrowReader failed", e);
        }
    }

    /** Encode a single batch into IPC stream bytes. */
    public static byte[] writeSingleBatch(VectorSchemaRoot root) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            try (ArrowStreamWriter w = new ArrowStreamWriter(root, null, Channels.newChannel(baos))) {
                w.start();
                w.writeBatch();
                w.end();
            }
            return baos.toByteArray();
        } catch (Exception e) {
            throw new RuntimeException("BatchUtil.writeSingleBatch failed", e);
        }
    }
}
