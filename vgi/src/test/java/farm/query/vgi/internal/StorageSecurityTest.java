// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.LegacyInit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import farm.query.vgi.Worker;
import farm.query.vgi.aggregate.AggregateFunction;
import farm.query.vgi.aggregate.StateCodec;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.AggregateBindRequest;
import farm.query.vgi.protocol.AggregateBindResponse;
import farm.query.vgi.protocol.AggregateDestructorRequest;
import farm.query.vgi.protocol.AggregateFinalizeRequest;
import farm.query.vgi.protocol.AggregateFinalizeResponse;
import farm.query.vgi.protocol.AggregateUpdateRequest;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.TableBufferingCombineRequest;
import farm.query.vgi.protocol.TableBufferingDestructorRequest;
import farm.query.vgi.protocol.TableBufferingProcessRequest;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.storage.SqliteFunctionStorage;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Storage security and round-trip budget for buffered and aggregate functions:
 * framework payloads are sealed (no secret reaches the SQLite file in
 * plaintext), the request's authenticated attach is always the one used,
 * aggregate state never goes through Java serialization, and the init /
 * argument caches cut the per-batch storage calls.
 */
class StorageSecurityTest {

    private static final byte[] SIGNING_KEY = "test-signing-key-0123456789".getBytes(StandardCharsets.UTF_8);
    private static final String SECRET_MARKER = "DUMMY-SECRET-7f3c9e1d";
    private static final String ARG_MARKER = "CONST-ARG-MARKER-51ab";
    private static final byte[] ATTACH_A = "attach-A-0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private static final byte[] ATTACH_B = "attach-B-0123456789abcdef".getBytes(StandardCharsets.UTF_8);
    private static final Schema OUT =
            new Schema(List.of(new Field("x", FieldType.nullable(new ArrowType.Int(64, true)), null)));

    @TempDir
    Path tmp;

    // ---------------------------------------------------------------- fixtures

    /** Buffers every batch; records the params it saw. */
    static final class Sink implements TableBufferingFunction {
        final List<TableBufferingProcessParams> seen = new ArrayList<>();
        @Override public String name() { return "t_sink"; }
        @Override public FunctionMetadata metadata() { return FunctionMetadata.describe("doc"); }
        @Override public List<ArgSpec> argumentSpecs() { return List.of(); }
        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUT));
        }
        @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
            seen.add(params);
            params.storage().stateAppend("buf".getBytes(StandardCharsets.UTF_8), new byte[0],
                    BatchUtil.writeSingleBatch(batch));
            return params.executionId();
        }
        @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
            return List.of(params.executionId());
        }
        @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
            throw new UnsupportedOperationException();
        }
    }

    /** {@code t_sum(value BIGINT)} with an explicit codec. */
    static final class SumAgg implements AggregateFunction<long[]> {
        private static final FunctionSpec SPEC = FunctionSpec.builder("t_sum").description("doc").build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public Schema outputSchema() { return Schemas.of(Schemas.nullable("result", Schemas.INT64)); }
        @Override public long[] newState() { return new long[1]; }
        @Override public void update(Map<Long, long[]> states, long[] gids, VectorSchemaRoot input) {
            BigIntVector v = (BigIntVector) input.getVector(0);
            for (int i = 0; i < input.getRowCount(); i++) {
                states.computeIfAbsent(gids[i], k -> new long[1])[0] += v.get(i);
            }
        }
        @Override public void combine(long[] t, long[] s) { t[0] += s[0]; }
        @Override public void finalize(FieldVector result, int row, long[] state) {
            ((BigIntVector) result).setSafe(row, state[0]);
        }
        @Override public byte[] serializeState(long[] s) {
            return StateCodec.encode(out -> out.writeLong(s[0]));
        }
        @Override public long[] deserializeState(byte[] b) {
            return StateCodec.decode(b, in -> new long[] {in.readLong()});
        }
    }

    /** Relies on the (removed) Java-serialization default. */
    static final class NoCodecAgg implements AggregateFunction<long[]> {
        private static final FunctionSpec SPEC = FunctionSpec.builder("t_nocodec").description("doc").build();
        @Override public FunctionSpec spec() { return SPEC; }
        @Override public Schema outputSchema() { return Schemas.of(Schemas.nullable("result", Schemas.INT64)); }
        @Override public long[] newState() { return new long[1]; }
        @Override public void update(Map<Long, long[]> states, long[] gids, VectorSchemaRoot input) {}
        @Override public void combine(long[] t, long[] s) {}
        @Override public void finalize(FieldVector result, int row, long[] state) {}
    }

    private static VgiServiceImpl service(Worker w, FunctionStorage storage) {
        return new VgiServiceImpl(w, List.of(), List.of(), List.of(), w.aggregates(),
                false, null, null, SIGNING_KEY, storage);
    }

    static byte[] longBatch(String[] names, long[][] cols) {
        List<Field> fields = new ArrayList<>();
        for (String n : names) fields.add(new Field(n, FieldType.nullable(new ArrowType.Int(64, true)), null));
        try (VectorSchemaRoot root = VectorSchemaRoot.create(new Schema(fields), Allocators.root())) {
            root.allocateNew();
            int rows = cols[0].length;
            for (int c = 0; c < names.length; c++) {
                BigIntVector v = (BigIntVector) root.getVector(c);
                for (int r = 0; r < rows; r++) v.setSafe(r, cols[c][r]);
            }
            root.setRowCount(rows);
            return BatchUtil.writeSingleBatch(root);
        }
    }

    private static byte[] fileBytes(Path dir) throws IOException {
        ByteArrayOutputStream all = new ByteArrayOutputStream();
        try (Stream<Path> files = Files.list(dir)) {
            for (Path p : files.toList()) all.write(Files.readAllBytes(p));
        }
        return all.toByteArray();
    }

    private static boolean contains(byte[] hay, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= hay.length; i++) {
            for (int j = 0; j < needle.length; j++) if (hay[i + j] != needle[j]) continue outer;
            return true;
        }
        return false;
    }

    /** Bind + sink-phase init for {@code t_sink} under {@code attach}; returns the execution id. */
    private static byte[] initBuffering(VgiServiceImpl svc, byte[] attach, byte[] secrets) {
        BindRequest bind = new BindRequest("t_sink", new byte[0], "TABLE_BUFFERING", SchemaUtil.serializeSchema(OUT),
                null, secrets, attach, null, true, null, null, null, null, List.of("main"), null, null);
        BindResponse br = svc.bind(bind, null);
        byte[] execId = HexId.newExecutionId();
        svc.init(LegacyInit.of(null, br.output_schema(), br.opaque_data(), null, null, null,
                "TABLE_BUFFERING", execId, null, null, null, null, null, null, null, null, null, null, null),
                null);
        return execId;
    }

    private static void process(VgiServiceImpl svc, byte[] execId, byte[] attach, long batch) {
        svc.table_buffering_process(new TableBufferingProcessRequest("t_sink", execId,
                longBatch(new String[] {"x"}, new long[][] {{batch, batch + 1}}), attach, null, batch,
                List.of("main")), null);
    }

    // ----------------------------------------------------------- buffering

    @Test
    void bufferingInitIsSealedAndSecretNeverPlaintextOnDisk() throws IOException {
        Path dir = Files.createDirectories(tmp.resolve("db"));
        Sink sink = new Sink();
        Worker w = Worker.builder().registerTableBuffering(sink);
        try (SqliteFunctionStorage storage = new SqliteFunctionStorage(dir.resolve("s.db").toString())) {
            VgiServiceImpl svc = service(w, storage);
            byte[] exec = initBuffering(svc, ATTACH_A, SECRET_MARKER.getBytes(StandardCharsets.UTF_8));
            process(svc, exec, ATTACH_A, 1);
            // The secrets still reach the function, from the sealed init.
            assertArrayEquals(SECRET_MARKER.getBytes(StandardCharsets.UTF_8), sink.seen.get(0).secrets());
            // The request's attach is what the function sees (nothing stored is preferred).
            assertArrayEquals(ATTACH_A, sink.seen.get(0).attachOpaqueData());
            // A second process (another "worker process") cold-reads and opens the sealed init.
            VgiServiceImpl other = service(w, storage);
            process(other, exec, ATTACH_A, 3);
            assertArrayEquals(SECRET_MARKER.getBytes(StandardCharsets.UTF_8), sink.seen.get(1).secrets());
        }
        byte[] disk = fileBytes(dir);
        assertTrue(disk.length > 0);
        assertFalse(contains(disk, SECRET_MARKER.getBytes(StandardCharsets.UTF_8)),
                "the resolved secret reached the SQLite file in plaintext");
        assertFalse(contains(disk, ATTACH_A), "the attach plaintext reached the SQLite file");
    }

    @Test
    void bufferingInitBoundToRequestAttach() {
        Sink sink = new Sink();
        Worker w = Worker.builder().registerTableBuffering(sink);
        FunctionStorage storage = new SqliteFunctionStorage(":memory:");
        VgiServiceImpl svc = service(w, storage);
        byte[] exec = initBuffering(svc, ATTACH_A, null);
        // Another attach naming the same execution: the sealed init is bound to
        // ATTACH_A's shard, so it fails authentication instead of serving A's context.
        VgiServiceImpl cold = service(w, storage);
        assertThrows(StoragePayloadSeal.StoragePayloadException.class, () -> process(cold, exec, ATTACH_B, 1));
        // And the warm process never serves a cache entry across attaches either.
        assertThrows(StoragePayloadSeal.StoragePayloadException.class, () -> process(svc, exec, ATTACH_B, 1));
        process(svc, exec, ATTACH_A, 1);
        assertEquals(1, sink.seen.size());
    }

    @Test
    void bufferingInitCacheHitMissAndEviction() {
        Sink sink = new Sink();
        Worker w = Worker.builder().registerTableBuffering(sink);
        CountingStorage storage = new CountingStorage(new SqliteFunctionStorage(":memory:"));
        VgiServiceImpl svc = service(w, storage);
        byte[] exec = initBuffering(svc, ATTACH_A, null);
        assertEquals(1, svc.bufferingInitCacheSize());

        // Hit: the warm process reads no init from storage.
        storage.reset();
        process(svc, exec, ATTACH_A, 1);
        assertEquals(0, storage.count("stateGetMany"), storage.snapshot().toString());

        // Miss: a cold process reads it once, then hits.
        VgiServiceImpl cold = service(w, storage);
        storage.reset();
        process(cold, exec, ATTACH_A, 1);
        process(cold, exec, ATTACH_A, 3);
        assertEquals(1, storage.count("stateGetMany"), storage.snapshot().toString());

        // Destructor evicts; afterwards the execution is unknown (storage authoritative).
        svc.table_buffering_destructor(new TableBufferingDestructorRequest("t_sink", exec, ATTACH_A, null,
                List.of("main")), null);
        assertEquals(0, svc.bufferingInitCacheSize());
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> process(svc, exec, ATTACH_A, 5));
        assertTrue(e.getMessage().contains("unknown execution_id"), e.getMessage());
    }

    @Test
    void bufferedStorageCallsPerBatch() {
        Sink sink = new Sink();
        Worker w = Worker.builder().registerTableBuffering(sink);
        CountingStorage storage = new CountingStorage(new SqliteFunctionStorage(":memory:"));
        VgiServiceImpl svc = service(w, storage);
        byte[] exec = initBuffering(svc, ATTACH_A, null);
        storage.reset();
        int batches = 4;
        for (int i = 0; i < batches; i++) process(svc, exec, ATTACH_A, i);
        svc.table_buffering_combine(new TableBufferingCombineRequest("t_sink", exec, List.of(exec), ATTACH_A,
                null, List.of("main")), null);
        System.out.println("[storage-calls] buffered " + batches + " batches + combine: " + storage.snapshot());
        // One call per batch (the function's own append); the init comes from the cache.
        assertEquals(batches, storage.total(), storage.snapshot().toString());
    }

    // ----------------------------------------------------------- aggregates

    private static AggregateBindResponse aggBind(VgiServiceImpl svc, byte[] attach) {
        return svc.aggregate_bind(new AggregateBindRequest("t_sum", ArgumentsEncoder.positionalArgs(ARG_MARKER),
                null, null, null, attach, List.of("main"), null), null);
    }

    private static void aggUpdate(VgiServiceImpl svc, byte[] exec, byte[] attach, long[] values, long[] gids) {
        svc.aggregate_update(new AggregateUpdateRequest("t_sum", exec,
                longBatch(new String[] {"value", "__vgi_group_id"}, new long[][] {values, gids}),
                attach, List.of("main")), null);
    }

    private static Map<Long, Long> aggFinalize(VgiServiceImpl svc, AggregateBindResponse b, byte[] attach,
                                               long... gids) {
        AggregateFinalizeResponse r = svc.aggregate_finalize(new AggregateFinalizeRequest("t_sum",
                b.execution_id(), longBatch(new String[] {"group_id"}, new long[][] {gids}),
                b.output_schema(), attach, List.of("main")), null);
        Map<Long, Long> out = new HashMap<>();
        try (VectorSchemaRoot root = BatchUtil.readSingleBatch(r.result_batch(), Allocators.root())) {
            BigIntVector v = (BigIntVector) root.getVector(0);
            for (int i = 0; i < gids.length; i++) out.put(gids[i], v.isNull(i) ? null : v.get(i));
        }
        return out;
    }

    @Test
    void aggregateArgsSealedAndCallsPerBatch() throws IOException {
        Path dir = Files.createDirectories(tmp.resolve("agg"));
        Worker w = Worker.builder().registerAggregate(new SumAgg());
        try (SqliteFunctionStorage sqlite = new SqliteFunctionStorage(dir.resolve("s.db").toString())) {
            CountingStorage storage = new CountingStorage(sqlite);
            VgiServiceImpl svc = service(w, storage);
            AggregateBindResponse b = aggBind(svc, ATTACH_A);
            storage.reset();
            int batches = 4;
            for (int i = 0; i < batches; i++) {
                aggUpdate(svc, b.execution_id(), ATTACH_A, new long[] {1, 2, 3}, new long[] {0, 1, 0});
            }
            System.out.println("[storage-calls] aggregate " + batches + " updates: " + storage.snapshot());
            // loadStates + saveStates; the constant arguments come from the cache.
            assertEquals(2L * batches, storage.total(), storage.snapshot().toString());

            // A cold process reads the sealed args once, then finalizes correctly.
            VgiServiceImpl cold = service(w, storage);
            assertEquals(Map.of(0L, 4L * batches, 1L, 2L * batches), aggFinalize(cold, b, ATTACH_A, 0, 1));
            cold.aggregate_destructor(new AggregateDestructorRequest("t_sum", b.execution_id(),
                    longBatch(new String[] {"group_id"}, new long[][] {{0, 1}}), ATTACH_A, List.of("main")), null);
        }
        assertFalse(contains(fileBytes(dir), ARG_MARKER.getBytes(StandardCharsets.UTF_8)),
                "aggregate constant arguments reached the SQLite file in plaintext");
    }

    @Test
    void aggregateArgsBoundToShard() {
        Worker w = Worker.builder().registerAggregate(new SumAgg());
        FunctionStorage storage = new SqliteFunctionStorage(":memory:");
        AggregateBindResponse b = aggBind(service(w, storage), ATTACH_A);
        VgiServiceImpl cold = service(w, storage);
        aggUpdate(cold, b.execution_id(), ATTACH_A, new long[] {1}, new long[] {0});
        VgiServiceImpl cold2 = service(w, storage);
        assertThrows(StoragePayloadSeal.StoragePayloadException.class,
                () -> aggUpdate(cold2, b.execution_id(), ATTACH_B, new long[] {1}, new long[] {0}));
    }

    @Test
    void aggregateArgsCacheHitMissAndEviction() {
        Worker w = Worker.builder().registerAggregate(new SumAgg());
        CountingStorage storage = new CountingStorage(new SqliteFunctionStorage(":memory:"));
        VgiServiceImpl svc = service(w, storage);
        AggregateBindResponse b = aggBind(svc, ATTACH_A);
        AggregateStateStore store = svc.aggregateStoreForTests();
        assertEquals(1, store.cachedArgsCount());
        var bound = store.bind(b.execution_id(), ATTACH_A);
        storage.reset();
        assertNotNull(store.loadArgs(bound));
        assertEquals(0, storage.total(), "hit reads nothing");
        store.deleteStates(bound, new long[0]);
        assertEquals(0, store.cachedArgsCount());
        storage.reset();
        assertNotNull(store.loadArgs(bound));
        assertEquals(1, storage.count("stateGetMany"), "miss reads storage once");
        assertNotNull(store.loadArgs(bound));
        assertEquals(1, storage.count("stateGetMany"), "then hits");
    }

    // ----------------------------------------------------- no Java serialization

    @Test
    void aggregateWithoutCodecRefusedAtRegistration() {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> Worker.builder().registerAggregate(new NoCodecAgg()));
        assertTrue(e.getMessage().contains("serializeState"), e.getMessage());
        assertThrows(UnsupportedOperationException.class, () -> new NoCodecAgg().deserializeState(new byte[8]));
        assertThrows(UnsupportedOperationException.class, () -> new NoCodecAgg().serializeState(new long[1]));
    }

    @Test
    void javaSerializedStateIsRejectedNotDeserialized() throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            oos.writeObject(new java.util.HashMap<String, String>(Map.of("k", "v")));
        }
        assertThrows(IllegalArgumentException.class, () -> new SumAgg().deserializeState(bos.toByteArray()));
    }

    @Test
    void noObjectInputStreamInMainSources() throws IOException {
        Path src = Path.of("src/main/java");
        assertTrue(Files.isDirectory(src), "run from the vgi module directory");
        List<String> offenders = new ArrayList<>();
        try (Stream<Path> files = Files.walk(src)) {
            for (Path p : files.filter(f -> f.toString().endsWith(".java")).toList()) {
                String text = Files.readString(p);
                if (text.contains("new ObjectInputStream") || text.contains("new java.io.ObjectInputStream")
                        || text.contains(".readObject(")) {
                    offenders.add(p.toString());
                }
            }
        }
        assertEquals(List.of(), offenders, "Java deserialization on the storage/network path");
    }
}
