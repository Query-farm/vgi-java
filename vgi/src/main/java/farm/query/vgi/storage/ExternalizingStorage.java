// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.storage;

import farm.query.vgirpc.vendor.BlobRef;
import farm.query.vgirpc.vendor.BlobScope;
import farm.query.vgirpc.vendor.VendorBlobClient;
import farm.query.vgirpc.vendor.VendorError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.IntFunction;

/**
 * A {@link FunctionStorage} that envelopes every value and keeps large ones in
 * the <em>blob vendor</em> ({@code VGI_BLOB_VENDOR_URL}), storing only a small
 * pointer in the wrapped backend. The {@code FunctionStorage} API does not
 * change. Wire-compatible with vgi-python's {@code ExternalizingStorage}.
 *
 * <h2>Envelope</h2>
 * <p>Every value the wrapper writes (work-queue items, {@code state*} values,
 * log items) is a {@link StorageEnvelope}. With a vendor client, a value of
 * {@code threshold} bytes or more is uploaded (purpose {@code fs}) <em>before</em>
 * the backend call and stored as a ~73-byte pointer; reads fetch it back and
 * verify its size and SHA-256 against the pointer. Without one every value is
 * inline, and when the backend has a per-value cap (the Cloudflare DO's 1 MiB)
 * a value whose envelope exceeds it is refused with
 * {@link StorageValueTooLargeException}. Counters ({@code stateCounter*}) are
 * not enveloped.</p>
 *
 * <h2>Where it applies</h2>
 * <p>See {@link #wrapFromEnv}: {@code cloudflare-do} is always wrapped, vendor
 * or not, so its stored format never depends on a per-process variable; any
 * other backend only with {@code VGI_STORAGE_EXTERNALIZE=1}, which then
 * requires the vendor.</p>
 *
 * <h2>Clean-up</h2>
 * <p>Blobs are uploaded under the vendor scope {@code (shard, "q.<hex execution
 * id>")} for queue items and {@code (shard, "s.<hex scope id>")} for state and
 * log values, with the idempotency key {@code <call uuid hex>.<index>}.
 * {@code queueClear} and {@code executionClear} delete the matching scope,
 * {@code stateDrain} deletes the blobs it drained and {@code queuePop} the blob
 * it claimed; {@code stateDelete} leaves blobs until the scope is cleared. All
 * clean-up is best effort, on a background thread, never on the query's
 * critical path.</p>
 *
 * <h2>Errors</h2>
 * <p>A pointer whose blob cannot be read raises a {@link StorageBlobException}
 * whose message follows the SDK-neutral contract (see that class).</p>
 *
 * <p>Thread-safe when the backend is.</p>
 */
public final class ExternalizingStorage implements FunctionStorage {

    private static final Logger LOG = LoggerFactory.getLogger("vgi.storage.blobs");

    /** Values of at least this many bytes go to the blob vendor (default). */
    public static final long DEFAULT_EXTERNALIZE_BYTES = 512L * 1024;
    /** Vendor base URL. */
    public static final String ENV_VENDOR_URL = "VGI_BLOB_VENDOR_URL";
    /** Wrap any backend when {@code 1}/{@code true}/{@code yes}/{@code on}. */
    public static final String ENV_EXTERNALIZE = "VGI_STORAGE_EXTERNALIZE";
    /** Externalization threshold in bytes. */
    public static final String ENV_EXTERNALIZE_BYTES = "VGI_STORAGE_EXTERNALIZE_BYTES";
    /** Alias of {@link #ENV_EXTERNALIZE_BYTES}. */
    public static final String ENV_EXTERNALIZE_BYTES_ALIAS = "VGI_CF_DO_EXTERNALIZE_BYTES";

    private static final int TRANSFER_PARALLELISM = 8;
    private static final int DECODE_BATCH = 64;
    private static final Set<String> TRUE = Set.of("1", "true", "yes", "on");
    private static final Set<String> MISSING_CODES = Set.of("not_found", "not_uploaded", "NoSuchKey");

    /** State shared by the root wrapper and its shard views. */
    private static final class Shared {
        final BlobStore blobs;
        final VendorBlobClient vendor;
        final long threshold;
        final Long maxValueBytes;
        private ExecutorService cleanup;
        private final List<Future<?>> pending = new ArrayList<>();

        Shared(BlobStore blobs, VendorBlobClient vendor, long threshold, Long maxValueBytes) {
            this.blobs = blobs;
            this.vendor = vendor;
            this.threshold = threshold;
            this.maxValueBytes = maxValueBytes;
        }

        synchronized void submit(Runnable task, String what) {
            if (cleanup == null) {
                cleanup = Executors.newFixedThreadPool(2, r -> {
                    Thread t = new Thread(r, "vgi-blob-cleanup");
                    t.setDaemon(true);
                    return t;
                });
            }
            pending.removeIf(Future::isDone);
            pending.add(cleanup.submit(() -> {
                try {
                    task.run();
                } catch (RuntimeException e) {
                    LOG.warn("blob vendor clean-up after {} failed: {}", what, e.getMessage());
                }
            }));
        }

        void flush() {
            List<Future<?>> snapshot;
            synchronized (this) {
                snapshot = new ArrayList<>(pending);
            }
            for (Future<?> f : snapshot) {
                try {
                    f.get();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return;
                } catch (ExecutionException e) {
                    // logged by the task
                }
            }
        }

        void close() {
            ExecutorService pool;
            synchronized (this) {
                pool = cleanup;
                cleanup = null;
            }
            if (pool != null) {
                pool.shutdown();
                try {
                    pool.awaitTermination(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
            if (blobs != null) blobs.close();
        }
    }

    private final FunctionStorage base;
    private final Shared shared;
    private final String shardKey;
    private final boolean root;

    /**
     * Wrap a backend.
     *
     * @param base          the backend that stores the envelopes
     * @param blobs         the vendor client (purpose {@code fs}), or {@code null}
     *                      to keep every value inline; closed by {@link #close()}
     * @param threshold     with a vendor, values of at least this many bytes are blobs
     * @param maxValueBytes the backend's per-value cap, or {@code null} for none;
     *                      {@code threshold} may not exceed it
     * @throws IllegalArgumentException if {@code threshold} is not positive or
     *     exceeds {@code maxValueBytes}
     */
    public ExternalizingStorage(FunctionStorage base, VendorBlobClient blobs, long threshold, Long maxValueBytes) {
        this(base, blobs == null ? null : BlobStore.of(blobs), blobs, threshold, maxValueBytes);
    }

    /**
     * Wrap a backend, keeping large values in any {@link BlobStore} (the vendor, or the
     * worker's own bucket through {@link BlobStore#of(farm.query.vgirpc.external.ObjectStore,
     * String, long, AutoCloseable, java.util.function.Consumer)}).
     *
     * @param base          the backend that stores the envelopes
     * @param blobs         where large values go, or {@code null} to keep every value inline;
     *                      closed by {@link #close()}
     * @param threshold     values of at least this many bytes are blobs
     * @param maxValueBytes the backend's per-value cap, or {@code null} for none
     * @return the wrapper
     * @throws IllegalArgumentException if {@code threshold} is not positive or exceeds
     *     {@code maxValueBytes}
     */
    public static ExternalizingStorage withBlobStore(FunctionStorage base, BlobStore blobs, long threshold,
                                                     Long maxValueBytes) {
        return new ExternalizingStorage(base, blobs, null, threshold, maxValueBytes);
    }

    private ExternalizingStorage(FunctionStorage base, BlobStore blobs, VendorBlobClient vendor, long threshold,
                                 Long maxValueBytes) {
        if (threshold < 1 || (maxValueBytes != null && threshold > maxValueBytes)) {
            throw new IllegalArgumentException("externalize threshold (" + ENV_EXTERNALIZE_BYTES
                    + ") must be between 1 and " + (maxValueBytes != null ? maxValueBytes : "unbounded")
                    + ", got " + threshold);
        }
        this.base = base;
        this.shared = new Shared(blobs, vendor, threshold, maxValueBytes);
        this.shardKey = "";
        this.root = true;
    }

    private ExternalizingStorage(FunctionStorage base, Shared shared, String shardKey) {
        this.base = base;
        this.shared = shared;
        this.shardKey = shardKey;
        this.root = false;
    }

    // ---------------------------------------------------------------------
    // Environment
    // ---------------------------------------------------------------------

    /**
     * Read {@value #ENV_EXTERNALIZE_BYTES} (alias {@value #ENV_EXTERNALIZE_BYTES_ALIAS}).
     *
     * @param env environment mapping
     * @return the threshold, or {@link #DEFAULT_EXTERNALIZE_BYTES} when unset
     * @throws IllegalArgumentException if the value is not a positive integer
     */
    public static long externalizeBytesFromEnv(Map<String, String> env) {
        for (String name : List.of(ENV_EXTERNALIZE_BYTES, ENV_EXTERNALIZE_BYTES_ALIAS)) {
            String raw = env.getOrDefault(name, "").strip();
            if (raw.isEmpty()) continue;
            long value;
            try {
                value = Long.parseLong(raw);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException(name + " must be an integer, got '" + raw + "'");
            }
            if (value <= 0) throw new IllegalArgumentException(name + " must be positive, got " + value);
            return value;
        }
        return DEFAULT_EXTERNALIZE_BYTES;
    }

    /**
     * Build the vendor client (purpose {@code fs}) from {@value #ENV_VENDOR_URL}
     * and its token ({@code VGI_BLOB_VENDOR_TOKEN}, falling back to
     * {@code VGI_CF_DO_TOKEN}).
     *
     * @param env environment mapping
     * @return the client, or {@code null} when {@value #ENV_VENDOR_URL} is unset
     * @throws IllegalArgumentException if the URL is set but no token is
     */
    public static VendorBlobClient blobClientFromEnv(Map<String, String> env) {
        if (env.getOrDefault(ENV_VENDOR_URL, "").strip().isEmpty()) return null;
        return VendorBlobClient.fromEnv("fs", env);
    }

    /**
     * Wrap {@code base} when the environment asks for it.
     *
     * <p>A backend with a per-value cap ({@code maxValueBytes}, i.e. the
     * Cloudflare DO) is always wrapped, vendor or not, so its stored format is
     * the same in every process. Any other backend is wrapped only with
     * {@value #ENV_EXTERNALIZE}{@code =1}, which then requires {@value #ENV_VENDOR_URL}.</p>
     *
     * @param base          the backend
     * @param maxValueBytes the backend's per-value cap, or {@code null} for none
     * @param env           environment mapping
     * @return {@code base} or the wrapper
     * @throws IllegalArgumentException if {@value #ENV_EXTERNALIZE}{@code =1} is set
     *     without a vendor, or a variable is malformed
     */
    public static FunctionStorage wrapFromEnv(FunctionStorage base, Long maxValueBytes, Map<String, String> env) {
        boolean enabled = TRUE.contains(env.getOrDefault(ENV_EXTERNALIZE, "").strip().toLowerCase(Locale.ROOT));
        if (maxValueBytes == null && !enabled) return base;
        // The vendor, or the worker's own bucket (VGI_STORAGE_BACKEND=s3|gcs), per the same
        // selection the HTTP server makes.
        BlobStore blobs = StorageBackend.blobStoreFromEnv(env, VendorBlobClient.DEFAULT_MAX_FETCH_BYTES);
        if (enabled && blobs == null) {
            throw new IllegalArgumentException(ENV_EXTERNALIZE + "=1 needs a storage backend: set " + ENV_VENDOR_URL
                    + ", or " + StorageBackend.ENV_S3_BUCKET + " / " + StorageBackend.ENV_GCS_BUCKET);
        }
        long threshold = externalizeBytesFromEnv(env);
        if (maxValueBytes != null) threshold = Math.min(threshold, maxValueBytes);
        return withBlobStore(base, blobs, threshold, maxValueBytes);
    }

    // ---------------------------------------------------------------------
    // Accessors
    // ---------------------------------------------------------------------

    /**
     * The wrapped backend.
     *
     * @return the backend
     */
    public FunctionStorage base() {
        return base;
    }

    /**
     * The vendor client, if any.
     *
     * @return the client, or {@code null}
     */
    public VendorBlobClient blobClient() {
        return shared.vendor;
    }

    /**
     * Values of at least this many bytes are externalized (with a vendor).
     *
     * @return the threshold in bytes
     */
    public long threshold() {
        return shared.threshold;
    }

    /** Wait for pending background blob clean-up (for tests and shutdown). */
    public void flushBlobCleanup() {
        shared.flush();
    }

    @Override
    public boolean requiresShardKey() {
        return base.requiresShardKey();
    }

    @Override
    public FunctionStorage forShard(String shard) {
        return new ExternalizingStorage(base.forShard(shard), shared, shard == null ? "" : shard);
    }

    /** Close the backend and, after pending clean-up finishes, the vendor client (root only). */
    @Override
    public void close() {
        if (!root) return;
        try {
            base.close();
        } finally {
            shared.close();
        }
    }

    // ---------------------------------------------------------------------
    // Encode / decode
    // ---------------------------------------------------------------------

    private static <R> List<R> parallelMap(int n, IntFunction<R> fn) {
        List<R> out = new ArrayList<>(n);
        if (n <= 1) {
            for (int i = 0; i < n; i++) out.add(fn.apply(i));
            return out;
        }
        Semaphore permits = new Semaphore(TRANSFER_PARALLELISM);
        List<Future<R>> futures = new ArrayList<>(n);
        try (ExecutorService pool = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                final int idx = i;
                futures.add(pool.submit(() -> {
                    permits.acquire();
                    try {
                        return fn.apply(idx);
                    } finally {
                        permits.release();
                    }
                }));
            }
            for (Future<R> f : futures) {
                try {
                    out.add(f.get());
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    futures.forEach(x -> x.cancel(true));
                    throw new StorageBlobException("storage blob transfer interrupted");
                } catch (ExecutionException e) {
                    futures.forEach(x -> x.cancel(true));
                    Throwable cause = e.getCause();
                    if (cause instanceof RuntimeException re) throw re;
                    if (cause instanceof Error err) throw err;
                    throw new IllegalStateException(cause);
                }
            }
        }
        return out;
    }

    /**
     * Envelope each value, uploading large ones first.
     *
     * @param values          the values to store
     * @param blobExecutionId the vendor scope's execution id
     * @return the envelopes, parallel to {@code values}
     * @throws StorageValueTooLargeException with no vendor, for an envelope over the backend's cap
     */
    List<byte[]> encodeValues(List<byte[]> values, String blobExecutionId) {
        BlobStore blobs = shared.blobs;
        int n = values.size();
        byte[][] envelopes = new byte[n][];
        List<Integer> large = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            byte[] v = values.get(i);
            if (blobs != null && v.length >= shared.threshold) {
                large.add(i);
            } else {
                envelopes[i] = StorageEnvelope.encodeInline(v);
                if (blobs == null && shared.maxValueBytes != null && envelopes[i].length > shared.maxValueBytes) {
                    throw new StorageValueTooLargeException("storage value of " + v.length
                            + " bytes exceeds the backend's " + shared.maxValueBytes + "-byte limit; set "
                            + ENV_VENDOR_URL + " (the blob vendor) so large values are stored as blobs");
                }
            }
        }
        if (!large.isEmpty()) {
            String callId = UUID.randomUUID().toString().replace("-", "");
            BlobScope scope = new BlobScope(StorageEnvelope.blobShard(shardKey), blobExecutionId);
            List<byte[]> pointers = parallelMap(large.size(), j -> {
                int i = large.get(j);
                return StorageEnvelope.encodePointer(blobs.putBytes(values.get(i), scope, callId + "." + i));
            });
            for (int j = 0; j < large.size(); j++) envelopes[large.get(j)] = pointers.get(j);
        }
        return List.of(envelopes);
    }

    /**
     * Decode envelopes, fetching pointers in parallel ({@code null} passes through).
     *
     * @param envelopes stored values ({@code null} for misses)
     * @param refsOut   when non-null, receives the blob references that were read
     * @return the values
     * @throws StorageEnvelopeException if a value is not a valid envelope, or a
     *     pointer was read with no vendor configured
     * @throws StorageBlobException if a pointer's blob is missing, fails its
     *     size or SHA-256 check, or cannot be fetched
     */
    List<byte[]> decodeValues(List<byte[]> envelopes, List<BlobRef> refsOut) {
        int n = envelopes.size();
        List<byte[]> out = new ArrayList<>(n);
        List<Integer> pointers = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            byte[] env = envelopes.get(i);
            if (env == null) {
                out.add(null);
            } else if (StorageEnvelope.tag(env) == StorageEnvelope.TAG_POINTER) {
                out.add(null);
                pointers.add(i);
            } else {
                out.add(StorageEnvelope.inlineValue(env));
            }
        }
        if (pointers.isEmpty()) return out;
        BlobStore blobs = shared.blobs;
        if (blobs == null) {
            throw new StorageEnvelopeException("stored value is a blob pointer but " + ENV_VENDOR_URL
                    + " is not configured in this process");
        }
        List<BlobRef> refs = new ArrayList<>(pointers.size());
        for (int i : pointers) refs.add(StorageEnvelope.decodePointer(envelopes.get(i)));
        List<byte[]> data = parallelMap(refs.size(), j -> fetch(blobs, refs.get(j)));
        for (int j = 0; j < pointers.size(); j++) out.set(pointers.get(j), data.get(j));
        if (refsOut != null) refsOut.addAll(refs);
        return out;
    }

    private static byte[] fetch(BlobStore blobs, BlobRef ref) {
        try {
            return blobs.getBytes(ref);
        } catch (VendorError e) {
            throw blobReadError(ref, e);
        }
    }

    /** Map a vendor client failure onto the storage-blob error contract. */
    static StorageBlobException blobReadError(BlobRef ref, VendorError e) {
        String code = e.code();
        Integer status = e.status();
        if (VendorError.SHA256_MISMATCH.equals(code)) {
            return new StorageBlobIntegrityException("storage blob " + ref.objectId()
                    + " failed its integrity check: sha256 mismatch");
        }
        if (VendorError.SIZE_MISMATCH.equals(code)) {
            return new StorageBlobIntegrityException("storage blob " + ref.objectId()
                    + " failed its integrity check: size mismatch (expected " + ref.size() + " bytes)");
        }
        if ((status != null && status == 404) || (code != null && MISSING_CODES.contains(code))) {
            return new StorageBlobMissingException("storage blob " + ref.objectId()
                    + " is missing: not found in the blob vendor (expired or deleted?)");
        }
        return new StorageBlobException("storage blob " + ref.objectId() + " could not be read: " + e.getMessage());
    }

    private List<KV> decodePairs(List<KV> rows, List<BlobRef> refsOut) {
        List<KV> result = new ArrayList<>(rows.size());
        for (int start = 0; start < rows.size(); start += DECODE_BATCH) {
            List<KV> chunk = rows.subList(start, Math.min(rows.size(), start + DECODE_BATCH));
            List<byte[]> values = decodeValues(chunk.stream().map(KV::value).toList(), refsOut);
            for (int i = 0; i < chunk.size(); i++) result.add(new KV(chunk.get(i).key(), values.get(i)));
        }
        return result;
    }

    // ---------------------------------------------------------------------
    // Background clean-up
    // ---------------------------------------------------------------------

    private void deleteBlobs(List<BlobRef> refs, String what) {
        BlobStore blobs = shared.blobs;
        if (blobs == null || refs.isEmpty()) return;
        List<BlobRef> copy = List.copyOf(refs);
        shared.submit(() -> blobs.deleteRefs(copy), what);
    }

    private void deleteBlobScope(String blobExecutionId, String what) {
        BlobStore blobs = shared.blobs;
        if (blobs == null) return;
        String shard = StorageEnvelope.blobShard(shardKey);
        shared.submit(() -> blobs.deleteScope(shard, blobExecutionId), what);
    }

    // ---------------------------------------------------------------------
    // FunctionStorage
    // ---------------------------------------------------------------------

    @Override
    public long stateAppend(byte[] scopeId, byte[] ns, byte[] key, byte[] value) {
        byte[] env = encodeValues(List.of(value), StorageEnvelope.blobExecutionId("s", scopeId)).get(0);
        return base.stateAppend(scopeId, ns, key, env);
    }

    @Override
    public List<LogEntry> stateLogScan(byte[] scopeId, byte[] ns, byte[] key, long afterId, int limit) {
        List<LogEntry> rows = base.stateLogScan(scopeId, ns, key, afterId, limit);
        List<byte[]> values = decodeValues(rows.stream().map(LogEntry::value).toList(), null);
        List<LogEntry> out = new ArrayList<>(rows.size());
        for (int i = 0; i < rows.size(); i++) out.add(new LogEntry(rows.get(i).id(), values.get(i)));
        return out;
    }

    @Override
    public List<byte[]> stateGetMany(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        return decodeValues(base.stateGetMany(scopeId, ns, keys), null);
    }

    @Override
    public void statePutMany(byte[] scopeId, byte[] ns, List<KV> items) {
        if (items.isEmpty()) {
            base.statePutMany(scopeId, ns, items);
            return;
        }
        List<byte[]> encoded = encodeValues(items.stream().map(KV::value).toList(),
                StorageEnvelope.blobExecutionId("s", scopeId));
        List<KV> rows = new ArrayList<>(items.size());
        for (int i = 0; i < items.size(); i++) rows.add(new KV(items.get(i).key(), encoded.get(i)));
        base.statePutMany(scopeId, ns, rows);
    }

    @Override
    public List<KV> stateScan(byte[] scopeId, byte[] ns, byte[] start, byte[] end, boolean reverse, int limit) {
        return decodePairs(base.stateScan(scopeId, ns, start, end, reverse, limit), null);
    }

    @Override
    public List<KV> stateDrain(byte[] scopeId, byte[] ns) {
        List<BlobRef> drained = new ArrayList<>();
        List<KV> rows = decodePairs(base.stateDrain(scopeId, ns), drained);
        // The drained rows are gone: their blobs are not read again.
        deleteBlobs(drained, "stateDrain");
        return rows;
    }

    /** Deletes rows; their blobs stay until the scope is cleared. */
    @Override
    public int stateDelete(byte[] scopeId, byte[] ns, List<byte[]> keys) {
        return base.stateDelete(scopeId, ns, keys);
    }

    /** Deletes a key range; its blobs stay until the scope is cleared. */
    @Override
    public int stateDeleteRange(byte[] scopeId, byte[] ns, byte[] start, byte[] end) {
        return base.stateDeleteRange(scopeId, ns, start, end);
    }

    @Override
    public long stateCounterGet(byte[] scopeId, byte[] ns, byte[] key) {
        return base.stateCounterGet(scopeId, ns, key);
    }

    @Override
    public long stateCounterAdd(byte[] scopeId, byte[] ns, byte[] key, long delta) {
        return base.stateCounterAdd(scopeId, ns, key, delta);
    }

    @Override
    public void stateCounterSet(byte[] scopeId, byte[] ns, byte[] key, long value) {
        base.stateCounterSet(scopeId, ns, key, value);
    }

    @Override
    public void stateCounterDelete(byte[] scopeId, byte[] ns, byte[] key) {
        base.stateCounterDelete(scopeId, ns, key);
    }

    @Override
    public int queuePush(byte[] executionId, List<byte[]> items) {
        return base.queuePush(executionId, encodeValues(items, StorageEnvelope.blobExecutionId("q", executionId)));
    }

    @Override
    public byte[] queuePop(byte[] executionId) {
        byte[] env = base.queuePop(executionId);
        if (env == null) return null;
        List<BlobRef> claimed = new ArrayList<>();
        byte[] value = decodeValues(java.util.Collections.singletonList(env), claimed).get(0);
        deleteBlobs(claimed, "queuePop");
        return value;
    }

    @Override
    public int queueClear(byte[] executionId) {
        int cleared = base.queueClear(executionId);
        deleteBlobScope(StorageEnvelope.blobExecutionId("q", executionId), "queueClear");
        return cleared;
    }

    @Override
    public int executionClear(byte[] scopeId) {
        int deleted = base.executionClear(scopeId);
        deleteBlobScope(StorageEnvelope.blobExecutionId("s", scopeId), "executionClear");
        return deleted;
    }
}
