// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.RowTransformFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.PortableStreamState;
import farm.query.vgirpc.http.StateSerializer;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Continued table-in-out output: the exchange's tick / "more" rules, the
 * pending flag's HTTP state-token round trip, and the startup checks on
 * {@code continuationState()} and {@code inputRowsPerCall()}.
 */
class ContinuedOutputTest {

    private static final Schema SCHEMA = new Schema(List.of(Schemas.nullable("n", Schemas.INT64)));

    /** Emits each input row's value, {@code pageSize} rows per reply. */
    public static final class Pager extends TableInOutExchangeState {
        public long[] values = new long[0];
        public int next;
        public int pageSize = 2;
        public boolean cacheOnResume;

        public Pager() {}

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            BigIntVector n = (BigIntVector) input.root().getVector("n");
            values = new long[input.root().getRowCount()];
            for (int i = 0; i < values.length; i++) values[i] = n.get(i);
            next = 0;
            emitPage(out, CacheControl.ttl(60).toMetadata());
        }

        @Override public void resume(OutputCollector out, CallContext ctx) {
            emitPage(out, cacheOnResume ? CacheControl.ttl(60).toMetadata() : null);
        }

        private void emitPage(OutputCollector out, Map<String, String> md) {
            int end = Math.min(values.length, next + pageSize);
            VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            for (int i = next; i < end; i++) v.setSafe(i - next, values[i]);
            root.setRowCount(end - next);
            next = end;
            out.emit(root, withMoreOutput(next < values.length, md));
        }
    }

    /** Says more without implementing resume(). */
    public static final class NoResume extends TableInOutExchangeState {
        public int unused;

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            out.emit(VectorSchemaRoot.create(SCHEMA, Allocators.root()), withMoreOutput(true, null));
        }
    }

    /**
     * Implements resume() but keeps no position of its own. Registration-only:
     * its empty hooks would be refused (no data batch) if it were ever driven.
     */
    public static final class Stateless extends TableInOutExchangeState {
        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {}
        @Override public void resume(OutputCollector out, CallContext ctx) {}
    }

    /** Says more on its input, then answers the tick with nothing: resume() returning early at end of data. */
    public static final class SilentResume extends TableInOutExchangeState {
        public boolean started;

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            started = true;
            out.emit(VectorSchemaRoot.create(SCHEMA, Allocators.root()), withMoreOutput(true, null));
        }

        @Override public void resume(OutputCollector out, CallContext ctx) {
            // "Nothing left": returns without emitting the last (empty) reply.
        }
    }

    /** A plain exchange that answers an input with nothing. */
    public static final class SilentInput extends TableInOutExchangeState {
        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {}
    }

    /** A declarable base for the subclass checks. */
    public static class OpenPager extends TableInOutExchangeState {
        public int next;

        public OpenPager() {}

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            out.emit(VectorSchemaRoot.create(SCHEMA, Allocators.root()));
        }

        @Override public void resume(OutputCollector out, CallContext ctx) {
            out.emit(VectorSchemaRoot.create(SCHEMA, Allocators.root()));
        }
    }

    /** A well-formed subclass of the declared state. */
    public static final class SubPager extends OpenPager {
        public int extra;
    }

    /** A subclass of the declared state that encodes its own state. */
    public static final class PortableSubPager extends OpenPager implements PortableStreamState {
        @Override public byte[] encode() { return new byte[0]; }
        @Override public void decode(byte[] data) {}
    }

    /** A subclass of the declared state with no no-arg constructor. */
    public static final class NoCtorSubPager extends OpenPager {
        public NoCtorSubPager(int next) { this.next = next; }
    }

    /** Encodes its own state, so the pending flag would not ride the token. */
    public static final class PortablePager extends TableInOutExchangeState implements PortableStreamState {
        public int next;

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {}
        @Override public void resume(OutputCollector out, CallContext ctx) {}
        @Override public byte[] encode() { return new byte[0]; }
        @Override public void decode(byte[] data) {}
    }

    /** Has no no-arg constructor, so the state token cannot rebuild it. */
    public static final class NoCtorPager extends TableInOutExchangeState {
        public int next;

        public NoCtorPager(int next) { this.next = next; }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {}
        @Override public void resume(OutputCollector out, CallContext ctx) {}
    }

    /** Emits a batch from its own allocator, then fails the turn. */
    public static final class EmitThenThrow extends TableInOutExchangeState {
        public transient BufferAllocator allocator;

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            out.emit(filled(allocator));
            throw new IllegalStateException("failed after emitting");
        }
    }

    private static VectorSchemaRoot filled(BufferAllocator allocator) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, allocator);
        root.allocateNew();
        BigIntVector v = (BigIntVector) root.getVector("n");
        for (int i = 0; i < 100; i++) v.setSafe(i, i);
        root.setRowCount(100);
        return root;
    }

    private static AnnotatedBatch input(long... values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
        root.allocateNew();
        BigIntVector v = (BigIntVector) root.getVector("n");
        for (int i = 0; i < values.length; i++) v.setSafe(i, values[i]);
        root.setRowCount(values.length);
        return new AnnotatedBatch(root, Map.of());
    }

    private static AnnotatedBatch tick() {
        VectorSchemaRoot root = VectorSchemaRoot.create(SCHEMA, Allocators.root());
        root.setRowCount(0);
        return new AnnotatedBatch(root, Map.of(TableInOutExchangeState.STATUS_KEY, TableInOutExchangeState.CONTINUE));
    }

    /** One exchange turn; returns the reply's rows and metadata, releasing every buffer. */
    private record Reply(List<Long> rows, Map<String, String> metadata) {}

    private static Reply turn(TableInOutExchangeState state, AnnotatedBatch in) {
        OutputCollector out = new OutputCollector(SCHEMA, null, false);
        try (in) {
            state.exchange(in, out, null);
            OutputCollector.Entry e = out.dataEntry();
            List<Long> rows = new ArrayList<>();
            BigIntVector v = (BigIntVector) e.root().getVector("n");
            for (int i = 0; i < e.root().getRowCount(); i++) rows.add(v.get(i));
            return new Reply(rows, e.customMetadata());
        } finally {
            for (OutputCollector.Entry e : out.entries()) e.root().close();
        }
    }

    @Test
    void continuesAcrossTicksAndCachesOnlyTheFirstReply() {
        Pager p = new Pager();
        Reply first = turn(p, input(1, 2, 3, 4, 5));
        assertEquals(List.of(1L, 2L), first.rows());
        assertTrue(TableInOutExchangeState.saysMore(first.metadata()));
        assertEquals("60", first.metadata().get(CacheControl.TTL_KEY));
        Reply second = turn(p, tick());
        assertEquals(List.of(3L, 4L), second.rows());
        assertTrue(TableInOutExchangeState.saysMore(second.metadata()));
        Reply last = turn(p, tick());
        assertEquals(List.of(5L), last.rows());
        assertFalse(TableInOutExchangeState.saysMore(last.metadata()));
        // The input's output is done: the next real input is valid again.
        assertEquals(List.of(7L), turn(p, input(7)).rows());
    }

    @Test
    void pendingOutputSurvivesTheHttpStateToken() {
        Pager p = new Pager();
        turn(p, input(1, 2, 3));
        Pager restored = StateSerializer.deserialize(StateSerializer.serialize(p), Pager.class);
        assertEquals(List.of(3L), turn(restored, tick()).rows());
        // A restored state still refuses real input while output is pending.
        Pager again = StateSerializer.deserialize(StateSerializer.serialize(p), Pager.class);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(again, input(9)));
        assertTrue(e.getMessage().contains("expected a continuation tick"), e.getMessage());
    }

    @Test
    void refusesATickWithNoOutputPending() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(new Pager(), tick()));
        assertTrue(e.getMessage().contains("no output is pending"), e.getMessage());
    }

    @Test
    void refusesATickWithRows() {
        Pager p = new Pager();
        turn(p, input(1, 2, 3));
        AnnotatedBatch rows = input(1);
        AnnotatedBatch withRows = new AnnotatedBatch(rows.root(),
                Map.of(TableInOutExchangeState.STATUS_KEY, TableInOutExchangeState.CONTINUE));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(p, withRows));
        assertTrue(e.getMessage().contains("must have no rows"), e.getMessage());
    }

    @Test
    void refusesCacheControlOnATicksReply() {
        Pager p = new Pager();
        p.cacheOnResume = true;
        turn(p, input(1, 2, 3));
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(p, tick()));
        assertTrue(e.getMessage().contains("cache control"), e.getMessage());
    }

    @Test
    void refusesMoreWithoutResume() {
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(new NoResume(), input(1)));
        assertTrue(e.getMessage().contains("does not implement resume()"), e.getMessage());
    }

    @Test
    void refusesMoreFromATableProducer() {
        TableProducerState producer = new TableProducerState() {
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                out.emit(VectorSchemaRoot.create(SCHEMA, Allocators.root()),
                        TableInOutExchangeState.withMoreOutput(true, null));
            }
        };
        OutputCollector out = new OutputCollector(SCHEMA, null, true);
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> producer.produce(out, null));
        assertTrue(e.getMessage().contains("only valid in a table-in-out exchange"), e.getMessage());
    }

    @Test
    void refusesATickAnsweredWithNoBatch() {
        SilentResume s = new SilentResume();
        OutputCollector first = new OutputCollector(SCHEMA, null, false);
        try (AnnotatedBatch in = input(1)) {
            s.exchange(in, first, null);
        } finally {
            for (OutputCollector.Entry e : first.entries()) e.root().close();
        }
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(s, tick()));
        assertTrue(e.getMessage().contains("no data batch was emitted"), e.getMessage());
        assertTrue(e.getMessage().contains("resume()"), e.getMessage());
    }

    @Test
    void refusesAnInputAnsweredWithNoBatch() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> turn(new SilentInput(), input(1, 2)));
        assertTrue(e.getMessage().contains("no data batch was emitted"), e.getMessage());
        assertTrue(e.getMessage().contains("onInputBatch()"), e.getMessage());
    }

    @Test
    void answersAZeroRowInputWithNothingPending() {
        Pager p = new Pager();
        Reply empty = turn(p, input());
        assertEquals(List.of(), empty.rows());
        assertFalse(TableInOutExchangeState.saysMore(empty.metadata()));
        // Nothing is pending afterwards: a tick is refused and real input runs.
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(p, tick()));
        assertTrue(e.getMessage().contains("no output is pending"), e.getMessage());
        assertEquals(List.of(4L), turn(p, input(4)).rows());
    }

    @Test
    void refuseReleasesEmittedBuffers() {
        try (BufferAllocator allocator = Allocators.root().newChildAllocator("refuse", 0, Long.MAX_VALUE)) {
            OutputCollector out = new OutputCollector(SCHEMA, null, false);
            out.emit(filled(allocator));
            assertTrue(allocator.getAllocatedMemory() > 0);
            IllegalStateException e = TableInOutExchangeState.refuse(out, "refused");
            assertEquals("refused", e.getMessage());
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void aFailedTurnReleasesWhatItEmitted() {
        try (BufferAllocator allocator = Allocators.root().newChildAllocator("throw", 0, Long.MAX_VALUE)) {
            EmitThenThrow s = new EmitThenThrow();
            s.allocator = allocator;
            OutputCollector out = new OutputCollector(SCHEMA, null, false);
            try (AnnotatedBatch in = input(1)) {
                IllegalStateException e = assertThrows(IllegalStateException.class, () -> s.exchange(in, out, null));
                assertEquals("failed after emitting", e.getMessage());
            }
            assertEquals(0, allocator.getAllocatedMemory());
        }
    }

    @Test
    void withMoreOutputFalseLeavesMetadataAlone() {
        Map<String, String> md = new HashMap<>(Map.of("k", "v"));
        assertEquals(md, TableInOutExchangeState.withMoreOutput(false, md));
        assertNull(TableInOutExchangeState.withMoreOutput(false, null));
        assertEquals(Map.of("k", "v", "vgi.status", "HAVE_MORE_OUTPUT"),
                TableInOutExchangeState.withMoreOutput(true, md));
    }

    // --- registration ---------------------------------------------------------

    private static final Schema OUT = new Schema(List.of(Schemas.nullable("i", Schemas.INT64)));

    /** A blended function with a configurable continuation and per-call cap. */
    private record Fn(String name, Class<? extends TableInOutExchangeState> state, long maxWorkers, Long rowsPerCall,
                      TableInOutExchangeState exchange) implements RowTransformFunction {
        @Override public FunctionSpec spec() {
            return FunctionSpec.builder(name).description("continuation probe").arg("n", Schemas.INT64).build();
        }
        @Override public Class<? extends TableInOutExchangeState> continuationState() { return state; }
        @Override public long maxWorkers() { return maxWorkers; }
        @Override public Long inputRowsPerCall() { return rowsPerCall; }
        @Override public BindResponse onBind(TableInOutBindParams params) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(OUT));
        }
        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) { return exchange; }
    }

    private static String refusal(TableInOutFunction fn) {
        return assertThrows(IllegalArgumentException.class, () -> TableInOutContracts.check(fn)).getMessage();
    }

    @Test
    void acceptsADeclaredContinuation() {
        TableInOutContracts.check(new Fn("ok", Pager.class, 0, 3L, null));
        TableInOutContracts.checkExchange(new Fn("ok", Pager.class, 0, null, null), new Pager());
    }

    @Test
    void refusesContinuationOnOneSharedConnection() {
        assertTrue(refusal(new Fn("serial", Pager.class, 1, null, null)).contains("maxWorkers() = 1"));
    }

    @Test
    void refusesAContinuationStateWithoutResume() {
        assertTrue(refusal(new Fn("nr", NoResume.class, 0, null, null)).contains("does not implement resume()"));
    }

    @Test
    void refusesAContinuationStateWithNoPosition() {
        assertTrue(refusal(new Fn("sl", Stateless.class, 0, null, null)).contains("resume position"));
    }

    @Test
    void refusesAContinuationStateThatEncodesItsOwnState() {
        assertTrue(refusal(new Fn("portable", PortablePager.class, 0, null, null)).contains("encodes its own state"));
    }

    @Test
    void refusesAContinuationStateWithoutANoArgConstructor() {
        assertTrue(refusal(new Fn("noctor", NoCtorPager.class, 0, null, null)).contains("no-arg constructor"));
    }

    @Test
    void checksTheRuntimeClassOfASubclassExchange() {
        Fn fn = new Fn("open", OpenPager.class, 0, null, null);
        TableInOutContracts.check(fn);
        TableInOutContracts.checkExchange(fn, new OpenPager());
        TableInOutContracts.checkExchange(fn, new SubPager());
        IllegalStateException portable = assertThrows(IllegalStateException.class,
                () -> TableInOutContracts.checkExchange(fn, new PortableSubPager()));
        assertTrue(portable.getMessage().contains("encodes its own state"), portable.getMessage());
        assertTrue(portable.getMessage().contains(PortableSubPager.class.getName()), portable.getMessage());
        IllegalStateException noCtor = assertThrows(IllegalStateException.class,
                () -> TableInOutContracts.checkExchange(fn, new NoCtorSubPager(1)));
        assertTrue(noCtor.getMessage().contains("no-arg constructor"), noCtor.getMessage());
    }

    @Test
    void refusesANonPositiveInputRowsPerCall() {
        assertTrue(refusal(new Fn("zero", null, 0, 0L, null)).contains("positive row count"));
    }

    @Test
    void refusesAnUndeclaredResumingExchange() {
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> TableInOutContracts.checkExchange(new Fn("undeclared", null, 0, null, null), new Pager()));
        assertTrue(e.getMessage().contains("continuationState()"), e.getMessage());
    }

    @Test
    void workerRefusesABadRegistrationAtStartup() {
        Worker w = Worker.builder().catalogName("cont").registerTableInOut("main", new Fn("serial", Pager.class, 1,
                null, null));
        assertThrows(IllegalArgumentException.class,
                () -> new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates()));
    }

    @Test
    void functionInfoCarriesInputRowsPerCall() {
        Worker w = Worker.builder().catalogName("cont")
                .registerTableInOut("main", new Fn("sliced", null, 0, 3L, null))
                .registerTableInOut("main", new Fn("whole", null, 0, null, null));
        VgiServiceImpl svc = new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
        byte[] attach = svc.catalog_attach(CatalogAttachRequest.of("cont", null, null, null), null)
                .attach_opaque_data();
        Map<String, Long> byName = new HashMap<>();
        for (byte[] item : svc.catalog_schema_contents_functions(attach, List.of("main"), null, null, null)
                .items()) {
            FunctionInfo info = RecordCodec.deserializeFromBytes(item, FunctionInfo.class);
            byName.put(info.name(), info.input_rows_per_call());
        }
        assertEquals(3L, byName.get("sliced"));
        assertTrue(byName.containsKey("whole"));
        assertNull(byName.get("whole"));
    }
}
