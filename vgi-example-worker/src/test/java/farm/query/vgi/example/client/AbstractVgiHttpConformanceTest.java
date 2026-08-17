// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.catalog.ColumnStatistics;
import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.ColumnStatisticsDecoder;
import farm.query.vgi.client.EncodedPushdownFilters;
import farm.query.vgi.client.FilterPredicate;
import farm.query.vgi.client.ProjectedColumns;
import farm.query.vgi.client.PushdownFiltersEncoder;
import farm.query.vgi.client.ScanFunctionArguments;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.client.TableFunctionRequests;
import farm.query.vgi.client.TableInfoDecoder;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CardinalityResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.CatalogAttachResult;
import farm.query.vgi.protocol.FunctionInfo;
import farm.query.vgi.protocol.GlobalInitResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.ItemsResponse;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.TableInfo;
import farm.query.vgi.protocol.TableScanFunctionGetResponse;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.RpcStream;
import farm.query.vgirpc.StreamState;
import farm.query.vgirpc.http.HttpRpcConnection;
import farm.query.vgirpc.http.HttpRpcStream;
import farm.query.vgirpc.marshal.Marshalling;
import farm.query.vgirpc.marshal.RecordCodec;
import farm.query.vgirpc.wire.Allocators;
import farm.query.vgirpc.wire.IpcStreamReader;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * One body of VGI protocol assertions, run over HTTP against <em>two</em>
 * independent worker implementations: vgi-java's example worker
 * ({@link JavaWorkerHttpConformanceTest}) and vgi-python's reference fixture
 * worker ({@link PythonWorkerHttpConformanceTest}).
 *
 * <h2>Why two workers</h2>
 *
 * <p>Every other HTTP test of this client talks to {@code HttpServer} from the
 * sibling vgi-rpc-java repo — Java client against Java server. A wrong shared
 * assumption passes that arrangement and still fails in the field, because both
 * halves were written from the same reading of the spec. vgi-python is the
 * reference implementation, and it has never seen this encoder; if the client
 * is right against it, the client is right.
 *
 * <p>Parameterisation is by subclass rather than by {@code @ParameterizedTest}
 * arguments so each leg gets its own container: one worker is started per class
 * and shared by its methods (a cold Python fixture costs ~20s to boot), the
 * Python leg can skip as a whole when the toolchain is absent, and a divergence
 * reads in the report as the same test name failing under exactly one class.
 * The assertions themselves live here and are physically shared — there is no
 * second copy to drift.
 *
 * <h2>What it covers</h2>
 *
 * <p>Attach, schema/function/table discovery, the catalog-table read path
 * ({@code catalog_table_get} → {@code catalog_table_scan_function_get} → scan),
 * bind/init/producer drain on decoded values, projection pushdown, filter
 * pushdown proven by row reduction, scalar exchange, the optimiser RPCs
 * (cardinality + statistics), and error propagation.
 *
 * <h2>What it does NOT cover</h2>
 *
 * <p>Three of the worker's function kinds are declared on {@link VgiService}
 * and reachable through the proxy, but have never been driven from Java and are
 * not driven here either: aggregates ({@code aggregate_bind} → {@code _update}
 * → {@code _combine} → {@code _finalize}), streaming table-in-out, and the
 * buffered Sink+Source lifecycle ({@code table_buffering_*}). Unlike the paths
 * below, none of them has a client-side encoder, so covering them is more than
 * adding assertions. vgi-python has a drift detector for exactly this gap
 * ({@code tests/conformance/test_function_inventory.py}: enumerate the catalog,
 * assert the client can reach every <em>category</em>); the Java client would
 * fail its aggregate row today. This test at least proves the
 * {@code AGGREGATE_FUNCTION} listing path works on both workers, so the gap is
 * invocation, not discovery.
 *
 * <h2>Fixture intersection</h2>
 *
 * <p>The two workers serve the same C++ integration suite, so their fixtures
 * agree — but that is checked at runtime rather than assumed: every function
 * these assertions call is asserted to appear in the worker's own
 * {@code catalog_schema_contents_functions} listing (see
 * {@link #advertisesEveryFixtureTheseAssertionsUse()}), so a fixture that
 * exists on only one side fails loudly instead of quietly testing nothing.
 */
abstract class AbstractVgiHttpConformanceTest {

    /** The catalog both workers advertise. */
    private static final String CATALOG = "example";

    /** Table functions this suite calls; asserted present on both workers. */
    private static final List<String> REQUIRED_TABLE_FUNCTIONS =
            List.of("sequence", "double_sequence", "projected_data", "filter_echo", "ten_thousand");

    /** Scalar functions this suite calls; asserted present on both workers. */
    private static final List<String> REQUIRED_SCALAR_FUNCTIONS = List.of("double");

    /** Catalog tables this suite reads; asserted present on both workers. */
    private static final List<String> REQUIRED_CATALOG_TABLES =
            List.of("numbers", "ten_thousand_table");

    private HttpRpcConnection connection;
    /** The proxy under test — {@code conn.proxy(VgiService.class)}. */
    protected VgiService vgi;
    private CatalogAttachResult attach;
    /** The attach handle every later call echoes back. */
    protected byte[] handle;

    /**
     * The worker this leg drives.
     *
     * @return the running worker
     */
    protected abstract VgiHttpWorkerUnderTest worker();

    @BeforeEach
    void connectAndAttach() {
        connection = HttpRpcConnection.builder(worker().endpoint())
                // A cold Python producer turn can take a while; the default is
                // already 5 min, this just pins it against future changes.
                .requestTimeout(Duration.ofMinutes(5))
                .build();
        vgi = connection.proxy(VgiService.class);
        attach = vgi.catalog_attach(attachRequest(), null);
        handle = attach.attach_opaque_data();
    }

    @AfterEach
    void disconnect() {
        if (connection != null) connection.close();
    }

    // ------------------------------------------------------------------
    // 1. catalog_attach
    // ------------------------------------------------------------------

    @Test
    @Timeout(120)
    void attachReturnsAHandleDefaultSchemaAndCapabilities() {
        // Confirm the catalog name rather than assume it: catalog_catalogs is
        // the discovery call a consumer makes before it can attach to anything,
        // and it is the only thing that says which catalogs a worker serves.
        List<String> catalogs = catalogNames();
        assertTrue(catalogs.contains(CATALOG),
                where("worker must serve the '" + CATALOG + "' catalog; got " + catalogs));

        assertNotNull(attach.attach_opaque_data(), where("attach must return a handle"));
        assertEquals("main", attach.default_schema(), where("default_schema"));
        // The capability flags the extension acts on. Both example catalogs
        // hand out transaction ids, carry time-travelling tables and publish
        // per-column statistics, so all three must read true — a flag decoded
        // off the wrong wire column would land on one of them.
        //
        // catalog_version_frozen is deliberately NOT asserted: the two fixtures
        // disagree (Java false, Python true) and both are defensible for a
        // catalog whose version never moves, so pinning either would be
        // asserting a fixture choice rather than the protocol.
        assertTrue(attach.supports_transactions(), where("supports_transactions"));
        assertTrue(attach.supports_time_travel(), where("supports_time_travel"));
        assertTrue(attach.supports_column_statistics(), where("supports_column_statistics"));
        assertEquals(1L, attach.catalog_version(), where("catalog_version"));
        assertNotNull(attach.tags(), where("tags map"));
        assertNotNull(attach.settings(), where("settings list"));
        assertFalse(attach.settings().isEmpty(),
                where("the example catalog declares session settings (greeting, multiplier, …)"));
    }

    // ------------------------------------------------------------------
    // 2. catalog_schemas
    // ------------------------------------------------------------------

    @Test
    @Timeout(120)
    void listsTheCatalogSchemas() {
        List<SchemaInfo> schemas = decodeItems(vgi.catalog_schemas(handle, null), SchemaInfo.class);
        List<String> names = schemas.stream().map(SchemaInfo::name).toList();
        assertTrue(names.contains("main"), where("schemas must include 'main', got " + names));
        assertTrue(names.contains("data"), where("schemas must include 'data', got " + names));
    }

    // ------------------------------------------------------------------
    // 3. catalog_schema_contents_functions
    // ------------------------------------------------------------------

    @Test
    @Timeout(120)
    void advertisesEveryFixtureTheseAssertionsUse() {
        Map<String, FunctionInfo> tables = functionsByName("main", "TABLE_FUNCTION");
        for (String name : REQUIRED_TABLE_FUNCTIONS) {
            assertTrue(tables.containsKey(name),
                    where("table function '" + name + "' is missing; the two workers must serve "
                            + "the same fixture set. Advertised: " + tables.keySet()));
        }
        Map<String, FunctionInfo> scalars = functionsByName("main", "SCALAR_FUNCTION");
        for (String name : REQUIRED_SCALAR_FUNCTIONS) {
            assertTrue(scalars.containsKey(name),
                    where("scalar function '" + name + "' is missing. Advertised: " + scalars.keySet()));
        }

        // The third function kind. Nothing below invokes an aggregate — the
        // Java client has no aggregate driver yet (see this class's Javadoc) —
        // but the listing path is a distinct dispatch on both workers and is
        // free to cover here, so a kind that stops being advertised is caught.
        Map<String, FunctionInfo> aggregates = functionsByName("main", "AGGREGATE_FUNCTION");
        assertFalse(aggregates.isEmpty(),
                where("the example catalog advertises aggregate functions"));

        // Spot-check the decoded record rather than just its name: schema_name
        // and function_type are dictionary-encoded on the wire, so a decode slip
        // shows up here and nowhere else.
        FunctionInfo sequence = tables.get("sequence");
        assertEquals("main", sequence.schema_name(), where("sequence.schema_name"));
        // Not a literal: the two workers legitimately spell this differently —
        // see VgiHttpWorkerUnderTest#tableFunctionTypeLabel.
        assertEquals(worker().tableFunctionTypeLabel(), sequence.function_type(),
                where("sequence.function_type"));
        assertFalse(sequence.description().isBlank(), where("sequence.description"));
        assertEquals(Boolean.TRUE, sequence.filter_pushdown(), where("sequence.filter_pushdown"));
        assertEquals(Boolean.TRUE, tables.get("projected_data").projection_pushdown(),
                where("projected_data.projection_pushdown"));

        // A table function's DISCOVERY-time output_schema is not asserted here.
        // vgi-python advertises an empty schema for every table function (the
        // shape depends on the bind arguments, so it only exists after a bind)
        // while vgi-java fills in the fixed one. Both are conformant — the C++
        // extension builds a scan from BindResponse.output_schema, not from
        // this field. The contractual schema is asserted at bind, in
        // bindsAndDrainsAProducerScan and projectionPushdownNarrows...().
        assertNotNull(sequence.output_schema(), where("sequence.output_schema present"));
        assertNotNull(SchemaUtil.deserializeSchema(sequence.output_schema()),
                where("sequence.output_schema must parse as an Arrow schema"));

        // max_workers is likewise left to the worker: vgi-python sends a null
        // (in a field its own schema calls non-nullable) when the function
        // declares no limit; vgi-java sends its default. The assertion that
        // matters is that a null decodes at all — a primitive int here used to
        // make every FunctionInfo from the reference implementation unreadable.
        assertTrue(sequence.max_workers() == null || sequence.max_workers() >= 1,
                where("sequence.max_workers"));
    }

    // ------------------------------------------------------------------
    // 4. catalog_schema_contents_tables + catalog_table_get
    //    + catalog_table_scan_function_get, then actually read the table
    // ------------------------------------------------------------------

    @Test
    @Timeout(180)
    void readsACatalogTableThroughItsScanFunction() {
        List<TableInfo> tables =
                TableInfoDecoder.decodeAll(vgi.catalog_schema_contents_tables(handle, "data", null, null).items());
        if (tables.isEmpty()) {
            fail(where("the worker exposes no catalog tables in schema 'data', so the "
                    + "TableCatalog read path cannot be covered here"));
        }
        List<String> names = tables.stream().map(TableInfo::name).toList();
        for (String required : REQUIRED_CATALOG_TABLES) {
            assertTrue(names.contains(required),
                    where("catalog table '" + required + "' is missing; got " + names));
        }
        TableInfo listed = tables.stream().filter(t -> t.name().equals("numbers")).findFirst().orElseThrow();
        assertEquals("data", listed.schema_name(), where("numbers.schema_name"));
        assertEquals(List.of("value"), fieldNames(SchemaUtil.deserializeSchema(listed.columns())),
                where("numbers.columns"));

        // catalog_table_get is the single-table read a catalog implementation
        // does on demand; it must agree with the listing.
        ItemsResponse fetched = vgi.catalog_table_get(handle, "data", "numbers", null, null, null, null);
        assertEquals(1, fetched.items().size(), where("catalog_table_get('numbers') item count"));
        TableInfo got = TableInfoDecoder.decode(fetched.items().get(0));
        assertEquals("numbers", got.name(), where("catalog_table_get name"));
        assertEquals("data", got.schema_name(), where("catalog_table_get schema_name"));
        Schema columns = SchemaUtil.deserializeSchema(got.columns());
        assertEquals(List.of("value"), fieldNames(columns), where("catalog_table_get columns"));
        assertEquals(ArrowType.ArrowTypeID.Int, columns.getFields().get(0).getType().getTypeID(),
                where("numbers.value type"));
        assertNotNull(got.comment(), where("numbers.comment"));

        // The scan function is how a table becomes rows. Both workers back
        // `numbers` with sequence(100). Its arguments come back in the flat
        // arg_N encoding, NOT the struct one a bind takes, so they have to be
        // transcoded — feeding them straight to bind is the plausible-looking
        // mistake ScanFunctionArguments exists to prevent.
        TableScanFunctionGetResponse scan =
                vgi.catalog_table_scan_function_get(handle, "data", "numbers", null, null, null, null);
        assertEquals("sequence", scan.function_name(), where("numbers scan function"));
        assertNotNull(scan.arguments(), where("numbers scan arguments"));
        assertTrue(scan.arguments().length > 0, where("numbers scan arguments must be non-empty"));

        ScanFunctionArguments.Decoded args = ScanFunctionArguments.decode(scan.arguments());
        assertEquals(1, args.positional().size(), where("numbers scan takes one positional argument"));
        assertEquals(100L, args.positional().get(0).value(), where("numbers scans sequence(100)"));

        List<Long> rows = drainInt64(
                scanRows("sequence", ScanFunctionArguments.toBindArguments(scan.arguments())), "n");
        assertEquals(100, rows.size(), where("numbers row count"));
        assertEquals(0L, rows.get(0), where("numbers first row"));
        assertEquals(99L, rows.get(99), where("numbers last row"));
    }

    // ------------------------------------------------------------------
    // 5. bind + init + producer drain, asserting decoded values
    // ------------------------------------------------------------------

    @Test
    @Timeout(180)
    void bindsAndDrainsAProducerScan() {
        BindRequest bind = tableBind("sequence", ArgumentsEncoder.positionalArgs(5L));
        BindResponse bound = vgi.bind(bind, null);
        Schema out = SchemaUtil.deserializeSchema(bound.output_schema());
        assertEquals(List.of("n"), fieldNames(out), where("sequence output schema"));

        RpcStream<? extends StreamState> stream = vgi.init(producerInit(bind, bound), null);
        GlobalInitResponse header = (GlobalInitResponse) stream.header();
        assertNotNull(header, where("init must return a GlobalInitResponse header"));
        assertNotNull(header.execution_id(), where("worker must mint an execution_id"));
        assertTrue(header.max_workers() >= 1, where("max_workers"));
        assertEquals(List.of(0L, 1L, 2L, 3L, 4L), drainInt64(stream, "n"), where("sequence(5)"));

        // A named FLOAT64 argument, so ArgumentsEncoder's named-value encoding
        // faces the other worker too — not just its positional int64 path.
        BindRequest doubles = tableBind("double_sequence",
                ArgumentsEncoder.builder().positional(5L).named("increment", 0.5d).encode());
        BindResponse doublesBound = vgi.bind(doubles, null);
        assertEquals(List.of(0.0d, 0.5d, 1.0d, 1.5d, 2.0d),
                drainFloat64(vgi.init(producerInit(doubles, doublesBound), null), "n"),
                where("double_sequence(5, increment := 0.5)"));

        // Several batches, so the drain loop crosses more than one producer turn
        // (and, over HTTP, more than one continuation request).
        BindRequest many = tableBind("sequence",
                ArgumentsEncoder.builder().positional(5000L).named("batch_size", 1000L).encode());
        BindResponse manyBound = vgi.bind(many, null);
        List<Long> rows = drainInt64(vgi.init(producerInit(many, manyBound), null), "n");
        assertEquals(5000, rows.size(), where("sequence(5000) row count"));
        assertEquals(4999L, rows.get(4999), where("sequence(5000) last row"));
    }

    // ------------------------------------------------------------------
    // 6. Projection pushdown
    // ------------------------------------------------------------------

    @Test
    @Timeout(180)
    void projectionPushdownNarrowsTheEmittedColumns() {
        BindRequest bind = tableBind("projected_data", ArgumentsEncoder.positionalArgs(4L));
        BindResponse bound = vgi.bind(bind, null);
        assertEquals(List.of("id", "name", "value", "extra"),
                fieldNames(SchemaUtil.deserializeSchema(bound.output_schema())),
                where("projected_data binds to its full schema"));

        // Ask for columns 0 and 2 only. The narrowing is the worker's job — bind
        // still reports the full schema — so the proof is in what it emits.
        InitRequest init = new InitRequest(
                RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                List.of(0, 2), null, null, null, null, null,
                null, null, null, null, null, null, null, null);
        RpcStream<? extends StreamState> stream = vgi.init(init, null);

        List<Map<String, Object>> rows = drainRows(stream);
        assertEquals(4, rows.size(), where("projected_data(4) row count"));
        assertEquals(List.of("id", "value"), List.copyOf(rows.get(0).keySet()),
                where("only the projected columns may be emitted"));
        assertEquals(List.of(0L, 1L, 2L, 3L), rows.stream().map(r -> r.get("id")).toList(),
                where("projected id values"));
        assertEquals(List.of(0.0d, 1.5d, 3.0d, 4.5d), rows.stream().map(r -> r.get("value")).toList(),
                where("projected value values"));
    }

    // ------------------------------------------------------------------
    // 7. Filter pushdown, end to end
    // ------------------------------------------------------------------

    @Test
    @Timeout(180)
    void filterPushdownReachesTheWorkerAndReducesRows() {
        // filter_echo emits (n, s, pushed_filters) and both workers auto-apply
        // the pushed predicate to the batch, so a correct encoding shows up
        // twice: as fewer rows, and as the predicate echoed back.
        ProjectedColumns cols = ProjectedColumns.of(List.of("n", "s", "pushed_filters"));
        EncodedPushdownFilters filters = PushdownFiltersEncoder.builder()
                .filter(cols.column("n"), FilterPredicate.ge(8L))
                .encode();

        List<Map<String, Object>> rows = scanFilterEcho(10L, filters);
        assertEquals(List.of(8L, 9L), rows.stream().map(r -> r.get("n")).toList(),
                where("n >= 8 must reduce filter_echo(10) to two rows"));
        assertEquals(List.of("row_8", "row_9"), rows.stream().map(r -> r.get("s")).toList(),
                where("surviving s values"));
        assertEquals("n >= 8", rows.get(0).get("pushed_filters"),
                where("the worker must see the predicate this encoder wrote"));

        // A conjunction, so the AND node shape faces a real decoder rather than
        // only the round-trip decoder in this repo.
        List<Map<String, Object>> ranged = scanFilterEcho(10L, PushdownFiltersEncoder.builder()
                .filter(cols.column("n"), FilterPredicate.and(
                        FilterPredicate.ge(3L), FilterPredicate.lt(6L)))
                .encode());
        assertEquals(List.of(3L, 4L, 5L), ranged.stream().map(r -> r.get("n")).toList(),
                where("3 <= n < 6"));

        // No filters at all: the same fixture must emit everything and say so,
        // which is what makes the reductions above meaningful.
        List<Map<String, Object>> unfiltered = scanFilterEcho(10L, null);
        assertEquals(10, unfiltered.size(), where("unfiltered filter_echo(10) row count"));
        assertEquals("(none)", unfiltered.get(0).get("pushed_filters"),
                where("unfiltered pushed_filters"));
    }

    // ------------------------------------------------------------------
    // 8. Scalar exchange
    // ------------------------------------------------------------------

    @Test
    @Timeout(180)
    void scalarExchangeComputesValues() {
        Schema input = new Schema(List.of(
                Field.nullable("value", new ArrowType.Int(64, true))));
        BindRequest bind = new BindRequest(
                "double", ArgumentsEncoder.builder().encode(), "SCALAR",
                SchemaUtil.serializeSchema(input), SettingsEncoder.builder().encode(), null,
                handle, null, false, null, null, null, null, "main");
        BindResponse bound = vgi.bind(bind, null);
        Schema out = SchemaUtil.deserializeSchema(bound.output_schema());
        assertEquals(List.of("result"), fieldNames(out), where("double's output column"));
        assertEquals(ArrowType.ArrowTypeID.Int, out.getFields().get(0).getType().getTypeID(),
                where("double(int64) stays integral"));

        RpcStream<? extends StreamState> stream = vgi.init(new InitRequest(
                RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                null, null, null, null, null, null,
                null, null, null, null, null, null, null, null), null);
        HttpRpcStream<?> session = (HttpRpcStream<?>) stream;
        try {
            // Several turns on one stream: a framing slip on turn N surfaces as
            // a decode failure or a hang on turn N+1, not on its own.
            assertEquals(List.of(2L, 20L, 200L), doubleAll(session, input, 1L, 10L, 100L),
                    where("double turn 1"));
            assertEquals(List.of(-10L), doubleAll(session, input, -5L), where("double turn 2"));
            assertEquals(List.of(0L, 84L), doubleAll(session, input, 0L, 42L), where("double turn 3"));
        } finally {
            session.close();
        }
    }

    // ------------------------------------------------------------------
    // 9. table_function_cardinality + table_function_statistics
    // ------------------------------------------------------------------

    @Test
    @Timeout(180)
    void reportsCardinalityAndStatisticsForABoundCall() {
        BindRequest bind = tableBind("sequence", ArgumentsEncoder.positionalArgs(500L));
        BindResponse bound = vgi.bind(bind, null);
        byte[] request = TableFunctionRequests.forBind(bind, bound.opaque_data());

        CardinalityResponse cardinality = vgi.table_function_cardinality(request);
        assertEquals(500L, cardinality.estimate(), where("sequence(500) cardinality estimate"));
        assertEquals(500L, cardinality.max(), where("sequence(500) cardinality max"));

        List<ColumnStatistics> stats =
                ColumnStatisticsDecoder.decode(vgi.table_function_statistics(request));
        assertEquals(1, stats.size(), where("sequence reports one column's statistics"));
        ColumnStatistics n = stats.get(0);
        assertEquals("n", n.columnName(), where("statistics column name"));
        assertEquals(new ArrowType.Int(64, true), n.arrowType(), where("statistics union member type"));
        assertEquals(0L, n.min(), where("statistics min"));
        assertEquals(499L, n.max(), where("statistics max"));
        assertEquals(false, n.hasNull(), where("statistics has_null"));
        assertTrue(n.hasNotNull(), where("statistics has_not_null"));
        assertEquals(500L, n.distinctCount(), where("statistics distinct_count"));

        // ten_thousand takes no arguments and hardcodes its cardinality — a
        // different code path on both workers from the count-derived one.
        BindRequest noArgs = tableBind("ten_thousand", ArgumentsEncoder.builder().encode());
        BindResponse noArgsBound = vgi.bind(noArgs, null);
        assertEquals(10000L,
                vgi.table_function_cardinality(
                        TableFunctionRequests.forBind(noArgs, noArgsBound.opaque_data())).estimate(),
                where("ten_thousand cardinality"));
    }

    // ------------------------------------------------------------------
    // 10. Error propagation
    // ------------------------------------------------------------------

    @Test
    @Timeout(180)
    void aFailedCallSurfacesAsRpcErrorAndLeavesTheConnectionUsable() {
        // Bind of a function that does not exist: a worker-side failure on a
        // UNARY call, so it must arrive as an RpcError carrying the worker's
        // own message — not as a transport-level "HTTP 500" with the cause left
        // behind in the server's log. Asserting the function name is what
        // separates the two: a generic status-code error cannot contain it.
        RpcError bindFailure = assertThrows(RpcError.class,
                () -> vgi.bind(tableBind("no_such_function_at_all",
                        ArgumentsEncoder.positionalArgs(1L)), null),
                where("binding an unknown function must fail"));
        assertTrue(bindFailure.errorMessage().contains("no_such_function_at_all"),
                where("the unary error must carry the worker's own message, got: "
                        + bindFailure.errorMessage()));
        assertFalse(bindFailure.errorType().isBlank(),
                where("the unary error must carry the worker's error type"));
        assertNotEquals("HttpError", bindFailure.errorType(),
                where("a worker error must not degrade to a transport error"));

        // Same connection, immediately after. HTTP has no shared socket to
        // desynchronise, but the client caches per-connection state (and a real
        // consumer will keep using it), so this is the assertion that a failure
        // left nothing behind.
        BindRequest ok = tableBind("sequence", ArgumentsEncoder.positionalArgs(3L));
        BindResponse bound = vgi.bind(ok, null);
        assertEquals(List.of(0L, 1L, 2L),
                drainInt64(vgi.init(producerInit(ok, bound), null), "n"),
                where("the connection must still work after a failed call"));

        // A mid-stream failure: generator_exception(0) raises on its first
        // producer turn, which over HTTP rides inside the /init response after
        // the stream header has already been written.
        RpcError streamFailure = assertThrows(RpcError.class, () -> {
            BindRequest boom = tableBind("generator_exception", ArgumentsEncoder.positionalArgs(0L));
            BindResponse boomBound = vgi.bind(boom, null);
            drainInt64(vgi.init(producerInit(boom, boomBound), null), "n");
        }, where("generator_exception(0) must fail the scan"));
        assertTrue(streamFailure.errorMessage().contains("Intentional failure"),
                where("the streaming error must carry the worker's own message, got: "
                        + streamFailure.errorMessage()));
        assertNotEquals("HttpError", streamFailure.errorType(),
                where("a worker error must not degrade to a transport error"));

        // And still usable after the streaming failure too.
        BindRequest after = tableBind("sequence", ArgumentsEncoder.positionalArgs(2L));
        BindResponse afterBound = vgi.bind(after, null);
        assertEquals(List.of(0L, 1L),
                drainInt64(vgi.init(producerInit(after, afterBound), null), "n"),
                where("the connection must still work after a streaming failure"));
    }

    // ==================================================================
    // Helpers
    // ==================================================================

    /** Tag an assertion message with the implementation, so a divergence names the side. */
    private String where(String message) {
        return "[" + worker().label() + "] " + message;
    }

    /**
     * The attach request the C++ extension sends for a plain
     * {@code ATTACH 'example' … (TYPE vgi)}: no options, no version pins. Empty
     * strings rather than nulls — that is what {@code InvokeCatalogAttach}
     * puts on the wire, and the fields are non-nullable.
     */
    private static CatalogAttachRequest attachRequest() {
        return new CatalogAttachRequest(CATALOG, new byte[0], "", "");
    }

    private BindRequest tableBind(String function, byte[] arguments) {
        return new BindRequest(
                function, arguments, "TABLE",
                null,                       // input_schema — producer mode
                SettingsEncoder.builder().encode(),
                null,                       // secrets
                handle, null, false,
                null, null,                 // at_unit / at_value
                null, null,                 // copy_from / copy_to
                "main");
    }

    private static InitRequest producerInit(BindRequest bind, BindResponse bound) {
        return new InitRequest(
                RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                null, null, null, null, null, null,
                null, null, null, null, null, null, null, null);
    }

    /** Bind and open a scan of {@code function} with already-encoded arguments. */
    private RpcStream<? extends StreamState> scanRows(String function, byte[] arguments) {
        BindRequest bind = tableBind(function, arguments);
        BindResponse bound = vgi.bind(bind, null);
        return vgi.init(producerInit(bind, bound), null);
    }

    /** One {@code filter_echo(count)} scan, optionally with pushed filters. */
    private List<Map<String, Object>> scanFilterEcho(long count, EncodedPushdownFilters filters) {
        BindRequest bind = tableBind("filter_echo", ArgumentsEncoder.positionalArgs(count));
        BindResponse bound = vgi.bind(bind, null);
        InitRequest init = new InitRequest(
                RecordCodec.serializeToBytes(bind), bound.output_schema(), bound.opaque_data(),
                null,
                filters == null ? null : filters.pushdownFilters(),
                filters == null ? null : filters.joinKeys(),
                null, null, null, null, null, null, null, null, null, null, null);
        return drainRows(vgi.init(init, null));
    }

    /**
     * The catalog names {@code catalog_catalogs} advertises.
     *
     * <p>Decoded field-by-field rather than into a record: vgi-java has a
     * {@code CatalogInfoSerializer} but no {@code CatalogInfo} type or decoder,
     * so a JVM consumer cannot read this response typed the way it reads
     * {@code FunctionInfo}. Only the name is needed here.
     */
    private List<String> catalogNames() {
        List<String> names = new ArrayList<>();
        for (byte[] item : vgi.catalog_catalogs().items()) {
            try (IpcStreamReader r = new IpcStreamReader(
                    new java.io.ByteArrayInputStream(item), Allocators.root())) {
                r.readNextBatch();
                Object name = Marshalling.decodeRow(r.root(), r.dictionaryProvider(), r.wireSchema())
                        .get("name");
                if (name != null) names.add(name.toString());
            } catch (Exception e) {
                throw new IllegalStateException("decoding a CatalogInfo item failed", e);
            }
        }
        return names;
    }

    private Map<String, FunctionInfo> functionsByName(String schema, String kind) {
        Map<String, FunctionInfo> out = new LinkedHashMap<>();
        for (FunctionInfo f : decodeItems(
                vgi.catalog_schema_contents_functions(handle, schema, kind, null, null),
                FunctionInfo.class)) {
            out.putIfAbsent(f.name(), f);
        }
        return out;
    }

    private static <R extends farm.query.vgirpc.schema.ArrowSerializableRecord> List<R> decodeItems(
            ItemsResponse response, Class<R> type) {
        List<R> out = new ArrayList<>(response.items().size());
        for (byte[] item : response.items()) out.add(RecordCodec.deserializeFromBytes(item, type));
        return out;
    }

    private static List<String> fieldNames(Schema schema) {
        return schema.getFields().stream().map(Field::getName).toList();
    }

    /**
     * Drive a producer stream to exhaustion, copying one int64 column out.
     *
     * <p>End of stream is a {@link NoSuchElementException} rather than a
     * sentinel, and an empty batch mid-stream means "keep going" — a producer
     * may legitimately answer a turn with no rows.
     */
    private static List<Long> drainInt64(RpcStream<? extends StreamState> stream, String column) {
        List<Long> out = new ArrayList<>();
        forEachBatch(stream, root -> {
            BigIntVector v = (BigIntVector) root.getVector(column);
            for (int i = 0; i < root.getRowCount(); i++) out.add(v.get(i));
        });
        return out;
    }

    private static List<Double> drainFloat64(RpcStream<? extends StreamState> stream, String column) {
        List<Double> out = new ArrayList<>();
        forEachBatch(stream, root -> {
            Float8Vector v = (Float8Vector) root.getVector(column);
            for (int i = 0; i < root.getRowCount(); i++) out.add(v.get(i));
        });
        return out;
    }

    /** Drain every batch into plain Java rows, preserving the emitted column order. */
    private static List<Map<String, Object>> drainRows(RpcStream<? extends StreamState> stream) {
        List<Map<String, Object>> out = new ArrayList<>();
        forEachBatch(stream, root -> {
            for (int i = 0; i < root.getRowCount(); i++) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (Field f : root.getSchema().getFields()) {
                    row.put(f.getName(), cell(root, f.getName(), i));
                }
                out.add(row);
            }
        });
        return out;
    }

    private static Object cell(VectorSchemaRoot root, String column, int row) {
        return switch (root.getVector(column)) {
            case BigIntVector v -> v.isNull(row) ? null : v.get(row);
            case Float8Vector v -> v.isNull(row) ? null : v.get(row);
            case VarCharVector v -> v.isNull(row) ? null : new String(v.get(row), StandardCharsets.UTF_8);
            default -> throw new IllegalStateException(
                    "no test decoder for column '" + column + "' of type "
                            + root.getVector(column).getField().getType());
        };
    }

    private static void forEachBatch(RpcStream<? extends StreamState> stream,
                                     java.util.function.Consumer<VectorSchemaRoot> consumer) {
        HttpRpcStream<?> session = (HttpRpcStream<?>) stream;
        try {
            while (true) {
                AnnotatedBatch batch;
                try {
                    batch = session.tick();
                } catch (NoSuchElementException endOfStream) {
                    break;
                }
                consumer.accept(batch.root());
            }
        } finally {
            session.close();
        }
    }

    /** One exchange turn over {@code double}, decoding the int64 result column. */
    private static List<Long> doubleAll(HttpRpcStream<?> session, Schema inputSchema, long... values) {
        try (VectorSchemaRoot input = VectorSchemaRoot.create(inputSchema, Allocators.root())) {
            input.allocateNew();
            BigIntVector v = (BigIntVector) input.getVector("value");
            for (int i = 0; i < values.length; i++) v.setSafe(i, values[i]);
            v.setValueCount(values.length);
            input.setRowCount(values.length);

            AnnotatedBatch answer = session.exchange(new AnnotatedBatch(input, null));
            VectorSchemaRoot root = answer.root();
            BigIntVector result = (BigIntVector) root.getVector("result");
            List<Long> out = new ArrayList<>(root.getRowCount());
            for (int i = 0; i < root.getRowCount(); i++) out.add(result.get(i));
            return out;
        }
    }
}
