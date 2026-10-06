// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.catalogcontents;

import farm.query.vgi.CatalogContentsEtag;
import farm.query.vgi.CatalogContentsProvider;
import farm.query.vgi.CatalogContentsResult;
import farm.query.vgi.Worker;
import farm.query.vgi.catalog.CatalogTable;
import farm.query.vgi.catalog.InMemoryCatalog;
import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.catalog.View;
import farm.query.vgi.example.aggregate.SumFunction;
import farm.query.vgi.example.scalar.DoubleFunction;
import farm.query.vgi.example.table.SequenceFunction;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.types.Schemas;

import java.util.List;
import java.util.Map;

/**
 * The six catalogs the {@code catalog_contents} integration tests attach
 * ({@code vgi/test/sql/integration/catalog/catalog_contents*.test}). A port of
 * vgi-python's {@code vgi/_test_fixtures/catalog_contents.py}, whose semantics
 * are the cross-SDK contract.
 *
 * <p>The same static two-schema catalog is served under three names, differing
 * only in how they answer {@code catalog_contents}:</p>
 * <ul>
 *   <li>{@code contents_probe} — advertises and serves it (version-frozen, no etag);</li>
 *   <li>{@code contents_broken} — advertises it, but {@code catalog_contents} fails, so
 *       the client must fall back to the per-schema RPCs;</li>
 *   <li>{@code contents_legacy} — does not advertise it, like an older worker.</li>
 * </ul>
 * <p>It holds every kind the client seeds from {@code catalog_contents} (tables, a
 * view, scalar / aggregate / table functions, scalar and table macros) over schemas
 * {@code main} and {@code extra}. Its functions are fresh instances of the example
 * catalog's {@code double} / {@code vgi_sum} / {@code sequence}, owned by each
 * catalog, so they are reachable only through its attach.</p>
 *
 * <p>Three DDL-capable {@link InMemoryCatalog}s advertise it too. Each ATTACH gets
 * its own empty catalog (one {@code main} schema), so tests sharing a warm worker
 * never see each other's objects:</p>
 * <ul>
 *   <li>{@code contents_memory} — reports {@code catalog_version} 0 and no etag
 *       (the client's version-0 rule);</li>
 *   <li>{@code contents_reval} — revalidates with a cheap etag, {@code gen-<version>},
 *       bumped by every DDL; a matching {@code if_none_match} is answered
 *       {@code not_modified} without building anything;</li>
 *   <li>{@code contents_hash} — no etag of its own, but the framework's
 *       {@link CatalogContentsEtag#CONTENT_HASH}.</li>
 * </ul>
 */
public final class CatalogContentsFixtures {

    /** Advertises and serves {@code catalog_contents}. */
    public static final String CATALOG_PROBE = "contents_probe";
    /** Advertises {@code catalog_contents} but fails to serve it. */
    public static final String CATALOG_BROKEN = "contents_broken";
    /** Does not advertise {@code catalog_contents}. */
    public static final String CATALOG_LEGACY = "contents_legacy";
    /** DDL-capable, version 0, no etag. */
    public static final String CATALOG_MEMORY = "contents_memory";
    /** DDL-capable, etag {@code gen-<version>}. */
    public static final String CATALOG_REVAL = "contents_reval";
    /** DDL-capable, content-hash etag. */
    public static final String CATALOG_HASH = "contents_hash";

    /** The error {@code contents_broken}'s {@code catalog_contents} raises (the tests match it). */
    public static final String BROKEN_MESSAGE = "contents_broken: catalog_contents deliberately fails";

    private CatalogContentsFixtures() {}

    /**
     * Serve all six catalogs from {@code w}.
     *
     * @param w the worker
     * @return {@code w}
     */
    public static Worker register(Worker w) {
        registerStatic(w, CATALOG_PROBE, true, null);
        registerStatic(w, CATALOG_BROKEN, true, request -> {
            throw new IllegalStateException(BROKEN_MESSAGE);
        });
        registerStatic(w, CATALOG_LEGACY, false, null);
        return w.registerCatalog(new ContentsMemoryCatalog())
                .registerCatalog(new ContentsRevalCatalog())
                .registerCatalog(new ContentsHashCatalog());
    }

    private static void registerStatic(Worker w, String name, boolean advertise,
                                       CatalogContentsProvider provider) {
        byte[] sequenceColumns = SchemaUtil.serializeSchema(
                Schemas.of(Schemas.nullable("n", Schemas.INT64)));
        w.registerExtraCatalog(new Worker.ExtraCatalog(name, null, null, "Every object kind")
                        .withCatalogComment("catalog_contents test catalog (" + name + ")")
                        .withSchemaComment("extra", "A second schema, tables only")
                        .withVersionFrozen(true)
                        .withCatalogContents(advertise, provider, CatalogContentsEtag.NONE))
                .registerExtraCatalogScalar(name, "main", new DoubleFunction())
                .registerExtraCatalogAggregate(name, "main", new SumFunction())
                .registerExtraCatalogTableFunction(name, "main", new SequenceFunction())
                .registerExtraCatalogTable(name, CatalogTable.builder("main", "ten", sequenceColumns)
                        .comment("Integers 0..9")
                        .scanFunction("sequence", List.of(10L), Map.of())
                        .build())
                .registerExtraCatalogTable(name, CatalogTable.builder("extra", "five", sequenceColumns)
                        .comment("Integers 0..4")
                        .scanFunction("sequence", List.of(5L), Map.of())
                        .build())
                .registerExtraCatalogView(name, new View("main", "answer", "SELECT 42 AS answer", "One row"))
                .registerExtraCatalogMacro(name, new Macro("main", "contents_triple", MacroType.SCALAR,
                        List.of("x"), Map.of(), Map.of(), "x * 3", "Triple a value", Map.of()))
                .registerExtraCatalogMacro(name, new Macro("main", "contents_range", MacroType.TABLE,
                        List.of("n"), Map.of(), Map.of(), "SELECT * FROM range(n)",
                        "Table macro over range(n)", Map.of()));
    }

    /** Private in-memory catalog reporting version 0 and no etag (the version-0 rule). */
    static final class ContentsMemoryCatalog extends InMemoryCatalog {
        ContentsMemoryCatalog() {
            super(CATALOG_MEMORY);
        }

        /** Always 0: the catalog does not track its version (still checks the attach). */
        @Override
        public long version(byte[] attachId) {
            super.version(attachId);
            return 0;
        }
    }

    /** Private in-memory catalog that revalidates with a generation-counter etag. */
    static final class ContentsRevalCatalog extends InMemoryCatalog {
        private static final CatalogContentsProvider GENERATION = CatalogContentsProvider.versionEtag();

        ContentsRevalCatalog() {
            super(CATALOG_REVAL);
        }

        /** {@code gen-<version>}: answer {@code not_modified} without building, else build. */
        @Override
        public CatalogContentsResult catalogContents(CatalogContentsProvider.Request request) {
            return GENERATION.catalogContents(request);
        }
    }

    /** Private in-memory catalog revalidated by the framework's content hash. */
    static final class ContentsHashCatalog extends InMemoryCatalog {
        ContentsHashCatalog() {
            super(CATALOG_HASH);
        }

        @Override
        public CatalogContentsEtag catalogContentsEtag() {
            return CatalogContentsEtag.CONTENT_HASH;
        }
    }
}
