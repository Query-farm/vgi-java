// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.accumulate;

import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.TimeStampMicroVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.TransferPair;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

/**
 * The persistent, ATTACH-scoped collection store behind the accumulate
 * fixtures, mirroring vgi-python's {@code _test_fixtures/accumulate/worker.py}.
 *
 * <p>A collection's rows live as append-only <em>segments</em> (one stamped
 * batch each) under a per-collection namespace, keyed by big-endian ingest time
 * plus a random suffix so keys sort by time: an append is one storage put, a
 * TTL is one ranged delete of the expired key range, and {@code max_row_size}
 * drops whole oldest segments trimming only the straddling one. The pinned
 * schema lives under a shared {@code meta} namespace; the row count is a
 * per-collection int64 counter keyed by collection name in that namespace.
 */
final class AccumulateStore {

    static final String TIMESTAMP_COLUMN = "_timestamp";
    // Plain (tz-naive) microsecond timestamp so it surfaces as DuckDB TIMESTAMP.
    static final org.apache.arrow.vector.types.pojo.ArrowType TIMESTAMP_TYPE =
            new org.apache.arrow.vector.types.pojo.ArrowType.Timestamp(
                    org.apache.arrow.vector.types.TimeUnit.MICROSECOND, null);

    static final byte[] META_NS = "meta".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SEG_NS_PREFIX = "seg:".getBytes(StandardCharsets.UTF_8);
    private static final SecureRandom RNG = new SecureRandom();
    private static final int MAX_NAME_BYTES = 255;

    private AccumulateStore() {}

    static void validateName(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("collection name must be a non-empty string");
        }
        if (name.getBytes(StandardCharsets.UTF_8).length > MAX_NAME_BYTES) {
            throw new IllegalArgumentException(
                    "collection name must be at most " + MAX_NAME_BYTES + " bytes");
        }
    }

    static Schema outputSchema(Schema input) {
        List<Field> fields = new ArrayList<>(input.getFields());
        fields.add(Field.nullable(TIMESTAMP_COLUMN, TIMESTAMP_TYPE));
        return new Schema(fields);
    }

    static Schema inputSchemaOf(Schema output) {
        List<Field> fields = new ArrayList<>();
        for (Field f : output.getFields()) {
            if (!TIMESTAMP_COLUMN.equals(f.getName())) fields.add(f);
        }
        return new Schema(fields);
    }

    /** Field name+type equality, ignoring metadata (Python's check_metadata=False). */
    static boolean schemasMatch(Schema a, Schema b) {
        if (a.getFields().size() != b.getFields().size()) return false;
        for (int i = 0; i < a.getFields().size(); i++) {
            Field fa = a.getFields().get(i);
            Field fb = b.getFields().get(i);
            if (!fa.getName().equals(fb.getName()) || !fa.getType().equals(fb.getType())) return false;
        }
        return true;
    }

    static byte[] segNs(byte[] name) {
        byte[] ns = new byte[SEG_NS_PREFIX.length + name.length];
        System.arraycopy(SEG_NS_PREFIX, 0, ns, 0, SEG_NS_PREFIX.length);
        System.arraycopy(name, 0, ns, SEG_NS_PREFIX.length, name.length);
        return ns;
    }

    /** Segment key: big-endian ingest time + random suffix, so keys sort by time. */
    static byte[] segKey(long callTsUs) {
        byte[] key = new byte[24];
        ByteBuffer.wrap(key).putLong(callTsUs);
        byte[] suffix = new byte[16];
        RNG.nextBytes(suffix);
        System.arraycopy(suffix, 0, key, 8, 16);
        return key;
    }

    static long nowUs() {
        Instant now = Instant.now();
        return now.getEpochSecond() * 1_000_000L + now.getNano() / 1_000L;
    }

    static Schema getSchema(BoundStorage ps, byte[] name) {
        byte[] blob = ps.stateGet(META_NS, name);
        return blob == null ? null : SchemaUtil.deserializeSchema(blob);
    }

    static void putSchema(BoundStorage ps, byte[] name, Schema outputSchema) {
        ps.statePut(META_NS, name, SchemaUtil.serializeSchema(outputSchema));
    }

    static long getCount(BoundStorage ps, byte[] name) {
        return ps.counterGet(META_NS, name);
    }

    /** Append one time-keyed segment (one storage put) and bump the row counter. */
    static void appendSegment(BoundStorage ps, byte[] name, byte[] batchIpc, long rows, long callTsUs) {
        ps.statePut(segNs(name), segKey(callTsUs), batchIpc);
        ps.counterAdd(META_NS, name, rows);
    }

    /** All segment blobs, oldest-first (the scan returns time-keyed order). */
    static List<byte[]> readSegments(BoundStorage ps, byte[] name) {
        List<byte[]> out = new ArrayList<>();
        for (FunctionStorage.KV kv : ps.stateScan(segNs(name), null, null, false, 0)) {
            out.add(kv.value());
        }
        return out;
    }

    private static long rowsOf(byte[] batchIpc) {
        try (VectorSchemaRoot root = BatchUtil.readSingleBatch(batchIpc, Allocators.root())) {
            return root.getRowCount();
        }
    }

    /** Drop segments whose ingest time predates {@code cutoffUs} — one ranged delete. */
    static void evictTtl(BoundStorage ps, byte[] name, long cutoffUs) {
        if (cutoffUs <= 0) return;
        byte[] end = ByteBuffer.allocate(8).putLong(cutoffUs).array();
        long removed = 0;
        for (FunctionStorage.KV kv : ps.stateScan(segNs(name), null, end, false, 0)) {
            removed += rowsOf(kv.value());
        }
        if (removed > 0) {
            ps.stateDeleteRange(segNs(name), null, end);
            ps.counterAdd(META_NS, name, -removed);
        }
    }

    /**
     * Drop the oldest rows until at most {@code maxRowSize} remain: whole oldest
     * segments, trimming only the one straddling the cap.
     */
    static void evictMaxRows(BoundStorage ps, byte[] name, long total, long maxRowSize) {
        long overflow = total - maxRowSize;
        long removed = 0;
        List<byte[]> deleteKeys = new ArrayList<>();
        byte[] trimKey = null;
        byte[] trimIpc = null;
        for (FunctionStorage.KV kv : ps.stateScan(segNs(name), null, null, false, 0)) {
            long rows = rowsOf(kv.value());
            if (removed + rows <= overflow) {
                removed += rows;
                deleteKeys.add(kv.key());
                if (removed == overflow) break;
            } else {
                int skip = (int) (overflow - removed);
                try (VectorSchemaRoot full = BatchUtil.readSingleBatch(kv.value(), Allocators.root());
                     VectorSchemaRoot kept = full.slice(skip, full.getRowCount() - skip)) {
                    trimIpc = BatchUtil.writeSingleBatch(kept);
                }
                trimKey = kv.key();
                removed = overflow;
                break;
            }
        }
        if (!deleteKeys.isEmpty()) ps.stateDelete(segNs(name), deleteKeys);
        if (trimKey != null) ps.statePut(segNs(name), trimKey, trimIpc);
        if (removed > 0) ps.counterAdd(META_NS, name, -removed);
    }

    /** Drop a collection (segments + pinned schema + counter); return rows removed. */
    static long clearCollection(BoundStorage ps, byte[] name) {
        long total = getCount(ps, name);
        ps.stateDeleteRange(segNs(name), null, null);
        ps.stateDelete(META_NS, List.of(name));
        ps.counterDelete(META_NS, name);
        return total;
    }

    /** Copy {@code input} and append the {@code _timestamp} column stamped with one call time. */
    static byte[] stampBatch(VectorSchemaRoot input, Schema outputSchema, long callTsUs) {
        int rows = input.getRowCount();
        try (VectorSchemaRoot out = VectorSchemaRoot.create(outputSchema, Allocators.root())) {
            for (int i = 0; i < input.getFieldVectors().size(); i++) {
                TransferPair tp = input.getVector(i).makeTransferPair(out.getVector(i));
                tp.transfer();
            }
            TimeStampMicroVector ts =
                    (TimeStampMicroVector) out.getVector(input.getFieldVectors().size());
            ts.allocateNew(rows);
            for (int r = 0; r < rows; r++) ts.set(r, callTsUs);
            ts.setValueCount(rows);
            out.setRowCount(rows);
            return BatchUtil.writeSingleBatch(out);
        }
    }
}
