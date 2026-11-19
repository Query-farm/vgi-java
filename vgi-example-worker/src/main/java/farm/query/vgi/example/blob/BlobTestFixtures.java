// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.blob;

import farm.query.vgi.Worker;
import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.BufferingStorageHolder;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgi.storage.BoundStorage;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.LargeVarCharVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.UUID;

/**
 * The {@code blob_test} catalog: functions sized by their arguments, for the
 * blob-vendor externalization tests ({@code test/sql/external_storage/} in the
 * vgi extension). The contract is the extension's
 * {@code docs/testing/blob-test-catalog.md}; vgi-python's
 * {@code vgi/_test_fixtures/blob_catalog.py} is the reference.
 *
 * <p>WARNING: TEST FUNCTIONS ONLY. A separate catalog
 * ({@code ATTACH 'blob_test' (TYPE vgi, ...)}) so the example catalog's
 * function counts, which every SDK mirrors, do not change.</p>
 *
 * <table>
 *   <caption>Functions</caption>
 *   <tr><th>Function</th><th>Kind</th><th>What it moves</th></tr>
 *   <tr><td>{@code blob_repeat(s, n)}</td><td>scalar</td><td>large output</td></tr>
 *   <tr><td>{@code blob_len(s)}</td><td>scalar</td><td>large input</td></tr>
 *   <tr><td>{@code blob_rows(rows, width)}</td><td>table</td><td>large table output</td></tr>
 *   <tr><td>{@code blob_param_echo(payload)}</td><td>table</td><td>large bind parameter</td></tr>
 *   <tr><td>{@code blob_buffer_echo(t)}</td><td>table-buffering</td><td>storage log values</td></tr>
 *   <tr><td>{@code blob_tio(t)}</td><td>table-in-out</td><td>storage state values</td></tr>
 *   <tr><td>{@code blob_store_values(t)}</td><td>table-in-out</td><td>exactly sized storage values</td></tr>
 * </table>
 *
 * <p>The checksum columns are {@code payload_len} (UTF-8 byte length) and
 * {@code payload_sha256} (lowercase hex SHA-256) of a value as text, matching
 * DuckDB's {@code strlen(x)} and {@code sha256(x)}; NULL gives NULL. The three
 * storage functions compute them from what storage returns, never from their
 * input, so the round trip is what gets tested.</p>
 */
public final class BlobTestFixtures {

    private BlobTestFixtures() {}

    /** The catalog name. */
    public static final String CATALOG_NAME = "blob_test";
    /** Its one schema. */
    public static final String SCHEMA = "main";

    static final int ROWS_PER_BATCH = 2048;
    static final byte[] BUFFER_NS = "blob_buffer".getBytes(StandardCharsets.UTF_8);
    static final byte[] TIO_NS = "blob_tio".getBytes(StandardCharsets.UTF_8);
    static final byte[] VALUES_NS = "blob_values".getBytes(StandardCharsets.UTF_8);
    static final byte[] LOG_KEY = new byte[0];

    /**
     * Register the {@code blob_test} catalog and its seven functions on {@code w}.
     *
     * @param w the worker
     * @return {@code w}
     */
    public static Worker register(Worker w) {
        return w.registerExtraCatalog(new Worker.ExtraCatalog(CATALOG_NAME, "vgi-fixture", "1.0.0",
                        "Externalization test functions", List.of(),
                        "Functions sized by their arguments, for blob externalization tests",
                        java.util.Map.of(), false, null, null, null))
                .registerExtraCatalogScalar(CATALOG_NAME, SCHEMA, new BlobRepeat())
                .registerExtraCatalogScalar(CATALOG_NAME, SCHEMA, new BlobLen())
                .registerExtraCatalogTableFunction(CATALOG_NAME, SCHEMA, new BlobRows())
                .registerExtraCatalogTableFunction(CATALOG_NAME, SCHEMA, new BlobParamEcho())
                .registerExtraCatalogTableBuffering(CATALOG_NAME, SCHEMA, new BlobBufferEcho())
                .registerExtraCatalogTableInOut(CATALOG_NAME, SCHEMA, new BlobTio())
                .registerExtraCatalogTableInOut(CATALOG_NAME, SCHEMA, new BlobStoreValues());
    }

    // ---------------------------------------------------------------------
    // Checksum helpers
    // ---------------------------------------------------------------------

    static final Field PAYLOAD_LEN = Schemas.nullable("payload_len", Schemas.INT64);
    static final Field PAYLOAD_SHA256 = Schemas.nullable("payload_sha256", Schemas.UTF8);

    static String sha256Hex(byte[] data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    /** The value at {@code i} as UTF-8 text (a string column as is), or {@code null}. */
    static byte[] text(FieldVector v, int i) {
        if (v.isNull(i)) return null;
        if (v instanceof VarCharVector s) return s.get(i);
        if (v instanceof LargeVarCharVector s) return s.get(i);
        return String.valueOf(v.getObject(i)).getBytes(StandardCharsets.UTF_8);
    }

    /** {@code inputFields} followed by the two checksum columns. */
    static Schema checksumSchema(List<Field> inputFields) {
        List<Field> fields = new ArrayList<>(inputFields);
        fields.add(PAYLOAD_LEN);
        fields.add(PAYLOAD_SHA256);
        return new Schema(fields);
    }

    static BindResponse bindChecksums(Schema in, boolean dropFirst) {
        List<Field> fields = in == null ? List.of() : in.getFields();
        if (dropFirst && !fields.isEmpty()) fields = fields.subList(1, fields.size());
        return BindResponse.forSchema(SchemaUtil.serializeSchema(checksumSchema(fields)));
    }

    /** Fill the two checksum vectors (the last two of {@code out}) from per-row values. */
    static void fillChecksums(VectorSchemaRoot out, List<byte[]> values) {
        int n = out.getFieldVectors().size();
        BigIntVector len = (BigIntVector) out.getVector(n - 2);
        VarCharVector sha = (VarCharVector) out.getVector(n - 1);
        len.allocateNew(values.size());
        sha.allocateNew(values.size());
        for (int i = 0; i < values.size(); i++) {
            byte[] v = values.get(i);
            if (v == null) {
                len.setNull(i);
                sha.setNull(i);
            } else {
                len.set(i, v.length);
                sha.setSafe(i, sha256Hex(v).getBytes(StandardCharsets.US_ASCII));
            }
        }
        len.setValueCount(values.size());
        sha.setValueCount(values.size());
    }

    /**
     * {@code stored}'s columns {@code from..} shared (reference-counted) into a
     * new root of {@code schema}, with the checksum columns of {@code values}.
     * {@code stored} is left intact and still owned by the caller.
     */
    static VectorSchemaRoot withChecksums(VectorSchemaRoot stored, int from, Schema schema, List<byte[]> values) {
        VectorSchemaRoot out = VectorSchemaRoot.create(schema, Allocators.root());
        int rows = stored.getRowCount();
        for (int c = from; c < stored.getFieldVectors().size(); c++) {
            stored.getVector(c).makeTransferPair(out.getVector(c - from)).splitAndTransfer(0, rows);
        }
        fillChecksums(out, values);
        out.setRowCount(rows);
        return out;
    }

    static List<byte[]> firstColumnText(VectorSchemaRoot root) {
        List<byte[]> values = new ArrayList<>(root.getRowCount());
        if (root.getFieldVectors().isEmpty()) {
            for (int i = 0; i < root.getRowCount(); i++) values.add(null);
            return values;
        }
        FieldVector col = root.getVector(0);
        for (int i = 0; i < root.getRowCount(); i++) values.add(text(col, i));
        return values;
    }

    static VectorSchemaRoot emptyRoot(Schema schema) {
        VectorSchemaRoot r = VectorSchemaRoot.create(schema, Allocators.root());
        r.allocateNew();
        r.setRowCount(0);
        return r;
    }

    // ---------------------------------------------------------------------
    // Scalars
    // ---------------------------------------------------------------------

    /** {@code blob_repeat(s VARCHAR, n BIGINT) -> VARCHAR}: {@code s} repeated {@code n} times. */
    public static final class BlobRepeat extends ScalarFn {
        @Override public String name() { return "blob_repeat"; }
        @Override public String description() {
            return "Repeat a string n times (externalization test: large scalar output)";
        }

        /**
         * Repeat each {@code s} {@code n} times ({@code n < 0} counts as 0); NULL in either gives NULL.
         *
         * @param s      strings to repeat
         * @param n      copy counts
         * @param result the output
         */
        public void compute(@Vector(doc = "String to repeat") VarCharVector s,
                            @Vector(doc = "Number of copies") BigIntVector n,
                            VarCharVector result) {
            int rows = s.getValueCount();
            result.allocateNew(rows);
            for (int i = 0; i < rows; i++) {
                if (s.isNull(i) || n.isNull(i)) {
                    result.setNull(i);
                    continue;
                }
                byte[] one = s.get(i);
                long copies = Math.max(0L, n.get(i));
                long total = copies * one.length;
                if (total > Integer.MAX_VALUE - 8L) {
                    throw new IllegalArgumentException("blob_repeat: result of " + total + " bytes is too large");
                }
                byte[] out = new byte[(int) total];
                for (int k = 0; k < copies; k++) System.arraycopy(one, 0, out, k * one.length, one.length);
                result.setSafe(i, out);
            }
            result.setValueCount(rows);
        }
    }

    /** {@code blob_len(s VARCHAR) -> BIGINT}: the UTF-8 byte length of {@code s}. */
    public static final class BlobLen extends ScalarFn {
        @Override public String name() { return "blob_len"; }
        @Override public String description() {
            return "UTF-8 byte length of a string (externalization test: large scalar input)";
        }

        /**
         * {@code strlen(s)} per row.
         *
         * @param s      strings to measure
         * @param result the output
         */
        public void compute(@Vector(doc = "String to measure") VarCharVector s, BigIntVector result) {
            int rows = s.getValueCount();
            result.allocateNew(rows);
            for (int i = 0; i < rows; i++) {
                if (s.isNull(i)) result.setNull(i);
                else result.set(i, s.getEndOffset(i) - s.getStartOffset(i));
            }
            result.setValueCount(rows);
        }
    }

    // ---------------------------------------------------------------------
    // Table functions
    // ---------------------------------------------------------------------

    /** {@code blob_rows(rows, width)}: row {@code i} is {@code repeat(chr(97 + i % 26), width)}. */
    public static final class BlobRows extends SimpleTableFunction {
        static final Schema OUTPUT = Schemas.of(
                Schemas.nullable("i", Schemas.INT64), Schemas.nullable("payload", Schemas.UTF8));
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_rows")
                .metadata(FunctionMetadata.describe("Rows of a given width (externalization test: large table output)"))
                .arg("rows", Schemas.INT64)
                .arg("width", Schemas.INT64)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }

        @Override public long cardinality(TableBindParams params) {
            Object rows = params.arguments().positional().isEmpty() ? null : params.arguments().positional().get(0);
            return rows instanceof Number num ? num.longValue() : -1L;
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            ParameterExtractor p = ParameterExtractor.of(params.arguments());
            long rows = p.positional(0, "rows").asLong().ge(0).required();
            long width = p.positional(1, "width").asLong().ge(0).required();
            return new State(rows, width);
        }

        /** Emits up to 2,048 rows per tick. Public fields + no-arg ctor for the HTTP state token. */
        public static final class State extends TableProducerState {
            /** Total rows. */
            public long rows;
            /** Characters per row. */
            public long width;
            /** Next row to emit. */
            public long next;

            /** No-arg constructor for state deserialization. */
            public State() {}

            State(long rows, long width) {
                this.rows = rows;
                this.width = width;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (next >= rows) {
                    out.finish();
                    return;
                }
                long start = next;
                int n = (int) Math.min(ROWS_PER_BATCH, rows - start);
                byte[][] letters = new byte[26][];
                BatchUtil.emit(OUTPUT, n, out, (root, count, ignored) -> {
                    BigIntVector iv = (BigIntVector) root.getVector("i");
                    VarCharVector pv = (VarCharVector) root.getVector("payload");
                    for (int k = 0; k < count; k++) {
                        long i = start + k;
                        int letter = (int) (i % 26);
                        if (letters[letter] == null) {
                            byte[] b = new byte[(int) width];
                            java.util.Arrays.fill(b, (byte) (97 + letter));
                            letters[letter] = b;
                        }
                        iv.setSafe(k, i);
                        pv.setSafe(k, letters[letter]);
                    }
                });
                next = start + n;
            }
        }
    }

    /** {@code blob_param_echo(payload)}: one row with the parameter's byte length and SHA-256. */
    public static final class BlobParamEcho extends SimpleTableFunction {
        static final Schema OUTPUT = Schemas.of(PAYLOAD_LEN, PAYLOAD_SHA256);
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_param_echo")
                .metadata(FunctionMetadata.describe(
                        "Length and SHA-256 of a parameter (externalization test: large bind parameter)"))
                .arg("payload", Schemas.UTF8)
                .build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return OUTPUT; }
        @Override public long cardinality(TableBindParams params) { return 1L; }

        @Override public TableProducerState createProducer(TableInitParams params) {
            ParameterExtractor p = ParameterExtractor.of(params.arguments());
            String payload = p.positional(0, "payload").asString().required();
            byte[] raw = payload.getBytes(StandardCharsets.UTF_8);
            return new State(raw.length, sha256Hex(raw));
        }

        /** The one row. Public fields + no-arg ctor for the HTTP state token. */
        public static final class State extends TableProducerState {
            /** UTF-8 byte length of the parameter. */
            public long length;
            /** Lowercase hex SHA-256 of the parameter. */
            public String sha256;
            /** Whether the row has been emitted. */
            public boolean done;

            /** No-arg constructor for state deserialization. */
            public State() {}

            State(long length, String sha256) {
                this.length = length;
                this.sha256 = sha256;
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (done) {
                    out.finish();
                    return;
                }
                done = true;
                BatchUtil.emit(OUTPUT, 1, out, (root, rows, ignored) -> {
                    ((BigIntVector) root.getVector(0)).setSafe(0, length);
                    ((VarCharVector) root.getVector(1)).setSafe(0, sha256.getBytes(StandardCharsets.US_ASCII));
                });
            }
        }
    }

    // ---------------------------------------------------------------------
    // Table-buffering
    // ---------------------------------------------------------------------

    /**
     * {@code blob_buffer_echo(t)}: process stores each input batch (IPC stream)
     * as one storage value appended to the execution's log; finalize replays
     * the log one stored batch per tick, with the checksum columns of its first
     * column.
     */
    public static final class BlobBufferEcho implements TableBufferingFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_buffer_echo")
                .metadata(FunctionMetadata.describe(
                        "Buffer input batches in storage and replay them (externalization test: storage values)"))
                .table("t")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return bindChecksums(params.inputSchema(), false);
        }

        @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
            params.storage().stateAppend(BUFFER_NS, LOG_KEY, BatchUtil.writeSingleBatch(batch));
            return params.executionId();
        }

        @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
            return List.of(params.executionId());
        }

        @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
            return new Replay(params);
        }

        /** Replays the log one entry per tick; {@code afterId} rides the HTTP state token. */
        public static final class Replay extends BufferingFinalizeProducer {
            /** Cursor: the id of the last entry emitted. */
            public long afterId = -1;

            /** No-arg constructor for state deserialization. */
            public Replay() {}

            Replay(TableBufferingFinalizeParams params) {
                super(params);
            }

            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                List<FunctionStorage.LogEntry> rows = storage().stateLogScan(BUFFER_NS, LOG_KEY, afterId, 1);
                if (rows.isEmpty()) {
                    out.finish();
                    return;
                }
                FunctionStorage.LogEntry e = rows.get(0);
                try (VectorSchemaRoot stored = BatchUtil.readSingleBatch(e.value(), Allocators.root())) {
                    List<byte[]> values = firstColumnText(stored);
                    VectorSchemaRoot full = withChecksums(stored, 0,
                            checksumSchema(stored.getSchema().getFields()), values);
                    emitProjected(full, out);
                    full.close();
                }
                afterId = e.id();
            }
        }
    }

    // ---------------------------------------------------------------------
    // Table-in-out
    // ---------------------------------------------------------------------

    /** A table-in-out state that re-binds its storage after an HTTP state-token round trip. */
    abstract static class StorageExchange extends TableInOutExchangeState {
        /** The execution id the storage view is scoped to. */
        public byte[] executionId;
        /** The attach id the storage view is sharded by, or {@code null}. */
        public byte[] attachId;
        /** The output schema. */
        public Schema outputSchema;
        private transient BoundStorage storageRef;

        StorageExchange() {}

        StorageExchange(TableInOutInitParams params) {
            this.executionId = params.storage().executionId();
            this.attachId = params.attachId();
            this.outputSchema = params.outputSchema();
            this.storageRef = params.storage();
        }

        BoundStorage storage() {
            if (storageRef == null) storageRef = BufferingStorageHolder.bind(executionId, attachId);
            return storageRef;
        }
    }

    /**
     * {@code blob_tio(t)}: stores each input batch (IPC stream) as one storage
     * value under a fresh key, reads it back, deletes it, and emits the batch as
     * read back with the checksum columns of its first column.
     */
    public static final class BlobTio implements TableInOutFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_tio")
                .metadata(FunctionMetadata.describe(
                        "Round-trip each batch through storage (externalization test: table-in-out state)"))
                .table("t")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return bindChecksums(params.inputSchema(), false);
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new State(params);
        }

        /** Per-batch store, read back, delete. */
        public static final class State extends StorageExchange {
            /** No-arg constructor for state deserialization. */
            public State() {}

            State(TableInOutInitParams params) {
                super(params);
            }

            @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                VectorSchemaRoot in = input.root();
                BoundStorage storage = storage();
                UUID u = UUID.randomUUID();
                byte[] key = ByteBuffer.allocate(16).putLong(u.getMostSignificantBits())
                        .putLong(u.getLeastSignificantBits()).array();
                storage.statePut(TIO_NS, key, BatchUtil.writeSingleBatch(in));
                byte[] stored = storage.stateGet(TIO_NS, key);
                if (stored == null) throw new IllegalStateException("blob_tio: the batch just stored is missing");
                storage.stateDelete(TIO_NS, List.of(key));
                try (VectorSchemaRoot back = BatchUtil.readSingleBatch(stored, Allocators.root())) {
                    out.emit(withChecksums(back, 0, outputSchema, firstColumnText(back)));
                }
            }
        }
    }

    /**
     * {@code blob_store_values(t)}: stores the UTF-8 bytes of each non-NULL
     * value of the first column as its own storage value of exactly that size,
     * reads them all back in one call, deletes them, and emits input columns
     * 2..n with the checksum columns of the bytes read back.
     */
    public static final class BlobStoreValues implements TableInOutFunction {
        private static final FunctionSpec SPEC = FunctionSpec.builder("blob_store_values")
                .metadata(FunctionMetadata.describe(
                        "Store each value of the first column exactly and read it back (externalization test)"))
                .table("t")
                .build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableInOutBindParams params) {
            return bindChecksums(params.inputSchema(), true);
        }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new State(params);
        }

        /** Per-batch put-many, get-many, delete. */
        public static final class State extends StorageExchange {
            /** No-arg constructor for state deserialization. */
            public State() {}

            State(TableInOutInitParams params) {
                super(params);
            }

            @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                VectorSchemaRoot in = input.root();
                List<byte[]> values = firstColumnText(in);
                UUID u = UUID.randomUUID();
                List<byte[]> keys = new ArrayList<>();
                List<FunctionStorage.KV> items = new ArrayList<>();
                List<Integer> rowsOf = new ArrayList<>();
                for (int i = 0; i < values.size(); i++) {
                    byte[] key = ByteBuffer.allocate(20).putLong(u.getMostSignificantBits())
                            .putLong(u.getLeastSignificantBits()).putInt(i).array();
                    if (values.get(i) != null) {
                        keys.add(key);
                        items.add(new FunctionStorage.KV(key, values.get(i)));
                        rowsOf.add(i);
                    }
                }
                List<byte[]> back = new ArrayList<>(values.size());
                for (int i = 0; i < values.size(); i++) back.add(null);
                if (!items.isEmpty()) {
                    BoundStorage storage = storage();
                    storage.statePutMany(VALUES_NS, items);
                    List<byte[]> stored = storage.stateGetMany(VALUES_NS, keys);
                    storage.stateDelete(VALUES_NS, keys);
                    for (int j = 0; j < stored.size(); j++) {
                        if (stored.get(j) == null) {
                            throw new IllegalStateException("blob_store_values: a value just stored is missing");
                        }
                        back.set(rowsOf.get(j), stored.get(j));
                    }
                }
                out.emit(withChecksums(in, 1, outputSchema, back));
            }
        }
    }
}
