// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.CatalogContentsResponse;
import farm.query.vgi.protocol.SchemaContents;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The framework rules every {@code catalog_contents} answer goes through
 * ({@link CatalogContents#respond}), the content-hash etag, and the
 * provider's short-circuit — mirroring vgi-python's
 * {@code Worker._catalog_contents_response} and {@code catalog_contents_digest}.
 */
final class CatalogContentsRulesTest {

    private static SchemaContents schema(String... path) {
        byte[] info = RecordCodec.serializeToBytes(
                new SchemaInfo(null, Map.of(), new byte[] {7}, List.of(path), null));
        return new SchemaContents(List.of(path), info,
                List.of(String.join(".", path).getBytes(StandardCharsets.UTF_8)),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
    }

    private static List<List<String>> paths(CatalogContentsResponse r) {
        return r.schemas().stream().map(SchemaContents::path).toList();
    }

    // ---- not_modified rules --------------------------------------------------

    @Test
    void notModifiedWithTheMatchingEtagIsPassedThrough() {
        CatalogContentsResponse r = CatalogContents.respond(5L,
                CatalogContentsResult.notModified("e1"), "e1", CatalogContentsEtag.NONE);
        assertTrue(r.not_modified());
        assertEquals("e1", r.etag());
        assertEquals(5L, r.catalog_version());
        assertTrue(r.schemas().isEmpty());
    }

    @Test
    void notModifiedWithoutAMatchingIfNoneMatchIsRejected() {
        assertThrows(IllegalStateException.class, () -> CatalogContents.respond(1L,
                CatalogContentsResult.notModified("e1"), null, CatalogContentsEtag.NONE));
        assertThrows(IllegalStateException.class, () -> CatalogContents.respond(1L,
                CatalogContentsResult.notModified("e1"), "e2", CatalogContentsEtag.NONE));
        assertThrows(IllegalStateException.class, () -> CatalogContents.respond(1L,
                new CatalogContentsResult(List.of(), null, true), "e1", CatalogContentsEtag.NONE));
    }

    @Test
    void notModifiedWithSchemasIsRejected() {
        assertThrows(IllegalStateException.class, () -> CatalogContents.respond(1L,
                new CatalogContentsResult(List.of(schema("a")), "e1", true), "e1", CatalogContentsEtag.NONE));
    }

    @Test
    void aFullAnswerWhoseEtagMatchesBecomesNotModified() {
        CatalogContentsResponse r = CatalogContents.respond(3L,
                CatalogContentsResult.of(List.of(schema("a")), "e1"), "e1", CatalogContentsEtag.NONE);
        assertTrue(r.not_modified());
        assertEquals("e1", r.etag());
        assertTrue(r.schemas().isEmpty());
    }

    @Test
    void aFullAnswerWithADifferentEtagIsSentInFull() {
        CatalogContentsResponse r = CatalogContents.respond(3L,
                CatalogContentsResult.of(List.of(schema("a")), "e2"), "e1", CatalogContentsEtag.NONE);
        assertFalse(r.not_modified());
        assertEquals("e2", r.etag());
        assertEquals(List.of(List.of("a")), paths(r));
    }

    @Test
    void noEtagMeansIfNoneMatchIsIgnored() {
        CatalogContentsResponse r = CatalogContents.respond(3L,
                CatalogContentsResult.of(List.of(schema("a"))), "e1", CatalogContentsEtag.NONE);
        assertFalse(r.not_modified());
        assertNull(r.etag());
        assertEquals(1, r.schemas().size());
    }

    // ---- paths ---------------------------------------------------------------

    @Test
    void schemasAreSentParentsFirstStably() {
        CatalogContentsResponse r = CatalogContents.respond(1L, CatalogContentsResult.of(List.of(
                schema("a", "b", "c"), schema("z"), schema("a", "b"), schema("a"))),
                null, CatalogContentsEtag.NONE);
        assertEquals(List.of(List.of("z"), List.of("a"), List.of("a", "b"), List.of("a", "b", "c")), paths(r));
    }

    @Test
    void duplicateOrphanedOrMismatchedPathsAreRejected() {
        assertThrows(IllegalStateException.class, () -> CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(schema("a"), schema("a"))), null, CatalogContentsEtag.NONE));
        assertThrows(IllegalStateException.class, () -> CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(schema("a", "b"))), null, CatalogContentsEtag.NONE));
        SchemaContents a = schema("a");
        SchemaContents mismatched = new SchemaContents(List.of("b"), a.schema(), a.tables(), a.views(),
                a.scalar_functions(), a.aggregate_functions(), a.table_functions(), a.scalar_macros(),
                a.table_macros(), a.indexes());
        assertThrows(IllegalStateException.class, () -> CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(mismatched)), null, CatalogContentsEtag.NONE));
    }

    // ---- content hash --------------------------------------------------------

    @Test
    void digestMatchesVgiPython() {
        // vgi.worker.catalog_contents_digest of the same snapshot (computed with
        // vgi-python cc83818), so a snapshot hashes alike in either SDK.
        SchemaContents a = new SchemaContents(List.of("a"), bytes("S"), List.of(bytes("t1"), bytes("t2")),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of());
        SchemaContents ab = new SchemaContents(List.of("a", "b"), bytes("X"), List.of(), List.of(), List.of(),
                List.of(), List.of(), List.of(new byte[0]), List.of(), List.of(bytes("i")));
        assertEquals("6a89e95ef4760072321eb1975cb2e541f2b53fecc75c2224b82cc0bd3b286c33",
                CatalogContents.digest(List.of(a, ab)));
        assertEquals("af5570f5a1810b7af78caf4bc70a660f0df51e42baf91d4de5b2328de0e83dfc",
                CatalogContents.digest(List.of()));
    }

    @Test
    void contentHashEtagIsDeterministicAndRevalidates() {
        CatalogContentsResponse first = CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(schema("b"), schema("a"))), null, CatalogContentsEtag.CONTENT_HASH);
        CatalogContentsResponse again = CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(schema("b"), schema("a"))), null, CatalogContentsEtag.CONTENT_HASH);
        assertEquals(64, first.etag().length());
        assertEquals(first.etag(), again.etag(), "same snapshot, same etag");
        assertEquals(CatalogContents.digest(first.schemas()), first.etag(), "hash of the wire-order snapshot");

        CatalogContentsResponse changed = CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(schema("a"))), null, CatalogContentsEtag.CONTENT_HASH);
        assertNotEquals(first.etag(), changed.etag());

        CatalogContentsResponse conditional = CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(schema("b"), schema("a"))), first.etag(),
                CatalogContentsEtag.CONTENT_HASH);
        assertTrue(conditional.not_modified());
        assertTrue(conditional.schemas().isEmpty());
        assertEquals(first.etag(), conditional.etag());
    }

    @Test
    void aCatalogEtagWinsOverTheContentHash() {
        CatalogContentsResponse r = CatalogContents.respond(1L,
                CatalogContentsResult.of(List.of(schema("a")), "mine"), null, CatalogContentsEtag.CONTENT_HASH);
        assertEquals("mine", r.etag());
    }

    // ---- provider ------------------------------------------------------------

    @Test
    void versionEtagShortCircuitsWithoutBuilding() {
        AtomicInteger builds = new AtomicInteger();
        CatalogContentsProvider provider = CatalogContentsProvider.versionEtag();
        CatalogContentsProvider.Request hit = new CatalogContentsProvider.Request(
                "cat", new byte[] {1}, 9L, "gen-9", null, () -> {
                    builds.incrementAndGet();
                    return List.of(schema("a"));
                });
        CatalogContentsResponse r = CatalogContents.respond(9L, provider.catalogContents(hit), "gen-9",
                CatalogContentsEtag.NONE);
        assertTrue(r.not_modified());
        assertEquals(0, builds.get(), "a matching validator must not build the snapshot");

        CatalogContentsProvider.Request miss = new CatalogContentsProvider.Request(
                "cat", new byte[] {1}, 10L, "gen-9", null, () -> {
                    builds.incrementAndGet();
                    return List.of(schema("a"));
                });
        CatalogContentsResponse full = CatalogContents.respond(10L, provider.catalogContents(miss), "gen-9",
                CatalogContentsEtag.NONE);
        assertFalse(full.not_modified());
        assertEquals("gen-10", full.etag());
        assertEquals(1, builds.get());
    }

    @Test
    void resultCopiesItsSchemaList() {
        List<SchemaContents> list = new ArrayList<>(List.of(schema("a")));
        CatalogContentsResult r = CatalogContentsResult.of(list, "e");
        list.clear();
        assertEquals(1, r.schemas().size());
        assertThrows(NullPointerException.class, () -> CatalogContentsResult.notModified(null));
    }

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }
}
