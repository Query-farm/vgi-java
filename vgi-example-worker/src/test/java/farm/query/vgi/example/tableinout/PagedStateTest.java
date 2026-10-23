// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.cache.CacheControl;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.tableinout.RowTransformFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.http.StateSerializer;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@link PagedFunctions.PagedState} against the fixture contracts in the vgi
 * extension's {@code test/sql/integration/table_in_out/} headers:
 * {@code lateral_continuation.test} (blended_paged), {@code continuation_classic.test}
 * (paged_explode), {@code input_rows_per_call.test} (blended_sliced) and
 * {@code continuation_resume_error_revalidate.test} (fail_on_page, revalidate,
 * not_modified_more). Driven through the real {@code exchange()} dispatch, a
 * tick per "more", so the framework's continued-output rules apply too.
 */
final class PagedStateTest {

    private static final Schema N = Schemas.of(Schemas.nullable("n", Schemas.INT64));
    private static final Schema TABLE_IN = Schemas.of(Schemas.nullable("x", Schemas.INT64));

    /** One reply: its rows, their parent rows (blended), call_rows (sliced), cache keys and "more". */
    private record Reply(List<Long> values, List<Integer> parents, List<Long> callRows,
                         Map<String, String> cache, boolean more) {}

    private static Arguments args(Object... kv) {
        Map<String, Object> named = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) named.put((String) kv[i], kv[i + 1]);
        return new Arguments(List.of(), named);
    }

    private static PagedFunctions.PagedState blended(Object... kv) {
        return new PagedFunctions.PagedState(PagedFunctions.PagedState.BLENDED, args(kv), 2);
    }

    private static PagedFunctions.PagedState table(Object... kv) {
        return new PagedFunctions.PagedState(PagedFunctions.PagedState.TABLE, args(kv), 2);
    }

    private static PagedFunctions.PagedState sliced(Object... kv) {
        return new PagedFunctions.PagedState(PagedFunctions.PagedState.SLICED, args(kv), 1_000_000);
    }

    /** A batch of counts; {@code null} is a NULL count. */
    private static VectorSchemaRoot counts(Schema schema, Long... values) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.allocateNew();
        BigIntVector v = (BigIntVector) root.getVector(0);
        for (int i = 0; i < values.length; i++) {
            if (values[i] == null) v.setNull(i);
            else v.setSafe(i, values[i]);
        }
        v.setValueCount(values.length);
        root.setRowCount(values.length);
        return root;
    }

    private static final Schema I = Schemas.of(Schemas.nullable("i", Schemas.INT64));
    private static final Schema I_CALL_ROWS = Schemas.of(Schemas.nullable("i", Schemas.INT64),
            Schemas.nullable("call_rows", Schemas.INT64));

    private static Reply turn(PagedFunctions.PagedState state, VectorSchemaRoot in, Map<String, String> md) {
        OutputCollector out = new OutputCollector(
                state.kind == PagedFunctions.PagedState.SLICED ? I_CALL_ROWS : I, null, false);
        try (in) {
            state.exchange(new AnnotatedBatch(in, md), out, null);
            OutputCollector.Entry e = out.dataEntry();
            VectorSchemaRoot root = e.root();
            List<Long> values = new ArrayList<>();
            List<Long> callRows = new ArrayList<>();
            BigIntVector iv = (BigIntVector) root.getVector("i");
            BigIntVector cv = (BigIntVector) root.getVector("call_rows");
            for (int r = 0; r < root.getRowCount(); r++) {
                values.add(iv.get(r));
                if (cv != null) callRows.add(cv.get(r));
            }
            Map<String, String> metadata = e.customMetadata() == null ? Map.of() : e.customMetadata();
            Map<String, String> cache = new HashMap<>();
            metadata.forEach((k, v) -> { if (k.startsWith("vgi.cache.")) cache.put(k, v); });
            return new Reply(values, parents(metadata.get(RowTransformFunction.PARENT_ROW_KEY)), callRows, cache,
                    TableInOutExchangeState.saysMore(metadata));
        } finally {
            for (OutputCollector.Entry e : out.entries()) e.root().close();
        }
    }

    private static List<Integer> parents(String b64) {
        List<Integer> out = new ArrayList<>();
        if (b64 == null) return out;
        ByteBuffer raw = ByteBuffer.wrap(Base64.getDecoder().decode(b64)).order(ByteOrder.LITTLE_ENDIAN);
        while (raw.hasRemaining()) out.add(raw.getInt());
        return out;
    }

    private static VectorSchemaRoot tickBatch(Schema schema) {
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
        root.setRowCount(0);
        return root;
    }

    private static final Map<String, String> TICK =
            Map.of(TableInOutExchangeState.STATUS_KEY, TableInOutExchangeState.CONTINUE);

    /** Every reply for one input batch: the input's, then one per tick while it says more. */
    private static List<Reply> drive(PagedFunctions.PagedState state, VectorSchemaRoot in, Map<String, String> md) {
        Schema schema = in.getSchema();
        List<Reply> replies = new ArrayList<>();
        replies.add(turn(state, in, md));
        while (replies.get(replies.size() - 1).more()) replies.add(turn(state, tickBatch(schema), TICK));
        return replies;
    }

    private static List<Reply> drive(PagedFunctions.PagedState state, Long... counts) {
        return drive(state, counts(N, counts), Map.of());
    }

    private static List<Long> values(List<Reply> replies) {
        List<Long> out = new ArrayList<>();
        for (Reply r : replies) out.addAll(r.values());
        return out;
    }

    // --- paging (lateral_continuation / continuation_classic) -----------------

    @Test
    void pagesTheFanOutAcrossInputRows() {
        // Row r with count n yields i = 0..n-1 (NULL and negative count as 0), cut
        // into page_size replies that may span rows; every reply but the last says more.
        List<Reply> replies = drive(blended("page_size", 2L), 3L, 0L, 2L, null, -1L, 1L);
        assertEquals(List.of(List.of(0L, 1L), List.of(2L, 0L), List.of(1L, 0L)),
                replies.stream().map(Reply::values).toList());
        assertEquals(List.of(List.of(0, 0), List.of(0, 2), List.of(2, 5)),
                replies.stream().map(Reply::parents).toList());
        assertEquals(List.of(true, true, false), replies.stream().map(Reply::more).toList());
    }

    @Test
    void everyPageSizeGivesBlendedExplodesRows() {
        List<Long> expected = List.of(0L, 1L, 2L, 3L, 4L, 0L, 0L, 1L);
        for (long size = 1; size <= 10; size++) {
            List<Reply> replies = drive(blended("page_size", size), 5L, 1L, 2L);
            assertEquals(expected, values(replies), "page_size " + size);
            assertEquals((expected.size() + size - 1) / size, replies.size(), "page_size " + size);
            for (Reply r : replies.subList(0, replies.size() - 1)) assertEquals(size, r.values().size());
        }
    }

    @Test
    void aBatchWithNoOutputIsOneEmptyReply() {
        assertEquals(List.of(new Reply(List.of(), List.of(), List.of(), Map.of(), false)),
                drive(blended(), 0L, null, -3L));
        assertEquals(1, drive(blended(), new Long[0]).size());
    }

    @Test
    void rowsWithNoOutputAfterTheLastPageAddNoReply() {
        List<Reply> replies = drive(blended("page_size", 2L), 2L, 0L, null, 0L);
        assertEquals(1, replies.size());
        assertEquals(List.of(0L, 1L), replies.get(0).values());
        assertFalse(replies.get(0).more());
    }

    @Test
    void cacheTtlRidesOnlyEachInputsFirstReply() {
        PagedFunctions.PagedState s = blended("page_size", 1L, "cache_ttl", 30L);
        for (int input = 0; input < 2; input++) {
            List<Reply> replies = drive(s, 3L);
            assertEquals("30", replies.get(0).cache().get(CacheControl.TTL_KEY));
            for (Reply r : replies.subList(1, replies.size())) assertEquals(Map.of(), r.cache());
        }
        for (Reply r : drive(blended("page_size", 1L), 3L)) assertEquals(Map.of(), r.cache());
    }

    @Test
    void theClassicFixtureReadsTheFirstColumnAndCarriesNoParentRows() {
        List<Reply> replies = drive(table("page_size", 2L, "cache_ttl", 5L), counts(TABLE_IN, 2L, null, 3L), Map.of());
        assertEquals(List.of(0L, 1L, 0L, 1L, 2L), values(replies));
        assertEquals(List.of(true, true, false), replies.stream().map(Reply::more).toList());
        for (Reply r : replies) assertEquals(List.of(), r.parents());
        assertEquals("5", replies.get(0).cache().get(CacheControl.TTL_KEY));
        assertEquals(Map.of(), replies.get(1).cache());
    }

    @Test
    void theResumePositionSurvivesTheStateToken() {
        PagedFunctions.PagedState s = blended("page_size", 2L);
        assertEquals(List.of(0L, 1L), turn(s, counts(N, 3L, 2L), Map.of()).values());
        PagedFunctions.PagedState restored = StateSerializer.deserialize(StateSerializer.serialize(s),
                PagedFunctions.PagedState.class);
        Reply second = turn(restored, tickBatch(N), TICK);
        assertEquals(List.of(2L, 0L), second.values());
        assertEquals(List.of(0, 1), second.parents());
        assertEquals(List.of(1L), turn(restored, tickBatch(N), TICK).values());
    }

    // --- blended_sliced (input_rows_per_call) ---------------------------------

    @Test
    void slicedReportsTheCallsRowCountAndRefusesMoreThanThreeRows() {
        List<Reply> replies = drive(sliced("page_size", 2L), 2L, 1L);
        assertEquals(List.of(0L, 1L, 0L), values(replies));
        for (Reply r : replies) for (long c : r.callRows()) assertEquals(2L, c);
        assertEquals(2, replies.size());
        assertThrows(IllegalStateException.class, () -> drive(sliced(), 1L, 1L, 1L, 1L));
    }

    @Test
    void slicedCacheTtlIsPerValueOnTheFirstReplyOnly() {
        List<Reply> replies = drive(sliced("page_size", 1L, "cache_ttl", 9L), 2L);
        assertEquals("9", replies.get(0).cache().get(CacheControl.TTL_KEY));
        assertEquals("1", replies.get(0).cache().get(CacheControl.PER_VALUE_KEY));
        assertEquals(Map.of(), replies.get(1).cache());
        assertEquals(Map.of(), drive(sliced(), 2L).get(0).cache());
    }

    // --- fail_on_page, revalidate, not_modified_more --------------------------

    @Test
    void failOnPageFailsThatReplyOfEachInput() {
        PagedFunctions.PagedState s = blended("page_size", 1L, "fail_on_page", 2L);
        assertEquals(List.of(0L), turn(s, counts(N, 3L), Map.of()).values());
        IllegalStateException e = assertThrows(IllegalStateException.class, () -> turn(s, tickBatch(N), TICK));
        assertEquals("failing on page 2 as asked", e.getMessage());

        IllegalStateException first = assertThrows(IllegalStateException.class,
                () -> drive(blended("fail_on_page", 1L), 1L));
        assertEquals("failing on page 1 as asked", first.getMessage());

        // Replies count per input batch: one-reply inputs never reach page 2.
        PagedFunctions.PagedState once = blended("page_size", 100L, "fail_on_page", 2L);
        for (int input = 0; input < 3; input++) assertEquals(1, drive(once, 4L).size());
    }

    @Test
    void revalidateAnswersAPlainRequestInOneReply() {
        List<Reply> replies = drive(blended("page_size", 1L, "cache_ttl", 99L, "revalidate", true), 3L, 2L);
        assertEquals(1, replies.size());
        assertEquals(List.of(0L, 1L, 2L, 0L, 1L), replies.get(0).values());
        assertEquals(List.of(0, 0, 0, 1, 1), replies.get(0).parents());
        assertEquals(Map.of(CacheControl.TTL_KEY, "0", CacheControl.ETAG_KEY, "v1",
                CacheControl.REVALIDATABLE_KEY, "1", CacheControl.SCOPE_KEY,
                replies.get(0).cache().get(CacheControl.SCOPE_KEY)), replies.get(0).cache());
    }

    @Test
    void revalidateAnswersAConditionalRequestWithChangedPages() {
        Map<String, String> conditional = Map.of(CacheControl.IF_NONE_MATCH_KEY, "v1");
        List<Reply> replies = drive(blended("page_size", 2L, "revalidate", true), counts(N, 3L, 1L), conditional);
        assertEquals(List.of(List.of(1000L, 1001L), List.of(1002L, 1000L)),
                replies.stream().map(Reply::values).toList());
        Map<String, String> first = replies.get(0).cache();
        assertEquals("0", first.get(CacheControl.TTL_KEY));
        assertEquals("v2", first.get(CacheControl.ETAG_KEY));
        assertEquals("1", first.get(CacheControl.REVALIDATABLE_KEY));
        assertNull(first.get(CacheControl.NOT_MODIFIED_KEY));
        assertEquals(Map.of(), replies.get(1).cache());
    }

    @Test
    void notModifiedMoreSendsAnEmptyNotModifiedReplyThatSaysMore() {
        Map<String, String> conditional = Map.of(CacheControl.IF_NONE_MATCH_KEY, "v1");
        List<Reply> replies = drive(blended("page_size", 2L, "revalidate", true, "not_modified_more", true),
                counts(N, 3L), conditional);
        Reply first = replies.get(0);
        assertEquals(List.of(), first.values());
        assertTrue(first.more());
        assertEquals("1", first.cache().get(CacheControl.NOT_MODIFIED_KEY));
        assertEquals("v1", first.cache().get(CacheControl.ETAG_KEY));
        assertEquals("0", first.cache().get(CacheControl.TTL_KEY));
        assertEquals("1", first.cache().get(CacheControl.REVALIDATABLE_KEY));
        assertEquals(List.of(List.of(1000L, 1001L), List.of(1002L)),
                replies.subList(1, replies.size()).stream().map(Reply::values).toList());

        // Without a conditional request it is plain revalidate: one reply, etag v1.
        List<Reply> plain = drive(blended("revalidate", true, "not_modified_more", true), 3L);
        assertEquals(1, plain.size());
        assertEquals("v1", plain.get(0).cache().get(CacheControl.ETAG_KEY));
        assertNull(plain.get(0).cache().get(CacheControl.NOT_MODIFIED_KEY));

        // The empty not_modified reply counts as page 1.
        PagedFunctions.PagedState failing = blended("revalidate", true, "not_modified_more", true,
                "fail_on_page", 2L);
        assertTrue(turn(failing, counts(N, 3L), conditional).more());
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> turn(failing, tickBatch(N), TICK));
        assertEquals("failing on page 2 as asked", e.getMessage());
    }

    // --- bind ----------------------------------------------------------------

    @Test
    void bindRefusesBadPageSizeAndCacheTtl() {
        PagedFunctions.BlendedPagedFunction fn = new PagedFunctions.BlendedPagedFunction();
        assertEquals("page_size must be at least 1, got 0", assertThrows(IllegalArgumentException.class,
                () -> fn.onBind(new TableInOutBindParams("blended_paged", args("page_size", 0L), N, Map.of())))
                .getMessage());
        assertEquals("cache_ttl must not be negative, got -1", assertThrows(IllegalArgumentException.class,
                () -> fn.onBind(new TableInOutBindParams("blended_paged", args("cache_ttl", -1L), N, Map.of())))
                .getMessage());
        // vgi.cache.ttl is an int32: a larger BIGINT is refused, not truncated.
        long tooBig = Integer.MAX_VALUE + 1L;
        assertTrue(assertThrows(IllegalArgumentException.class,
                () -> fn.onBind(new TableInOutBindParams("blended_paged", args("cache_ttl", tooBig), N, Map.of())))
                .getMessage().contains("cache_ttl must be at most"));
        fn.onBind(new TableInOutBindParams("blended_paged", args("cache_ttl", (long) Integer.MAX_VALUE), N,
                Map.of()));
    }
}
