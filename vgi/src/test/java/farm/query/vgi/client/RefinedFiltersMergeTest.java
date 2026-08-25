// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.CatalogAttachRequest;
import farm.query.vgi.protocol.ScanSplit;
import farm.query.vgi.protocol.TableFunctionPlanRequest;
import farm.query.vgi.table.PlanRequest;
import farm.query.vgi.table.PlanResult;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * {@code table_function_plan}'s {@code pushdown_filters} and {@code
 * refined_filters} are two separate wire fields that a single request may
 * carry together — a Trino-style client sends the scan's static constraint
 * as {@code pushdown_filters} on its first {@code plan()} call, then narrows
 * further on a later page purely via {@code refined_filters} (a dynamic
 * filter it awaited, e.g.), or a client that always resends everything it
 * knows might set both on the same call. Either way, a {@code plan()} author
 * should see ONE unified filter set for a given call, not have to know which
 * of two wire fields carried which fragment.
 *
 * <p>Before the fix alongside this test, {@code VgiServiceImpl#planRequestOf}
 * decoded {@code pushdown_filters} only and silently dropped {@code
 * refined_filters}/{@code filters_complete} entirely — any JVM-hosted
 * worker's {@code plan()} never saw a continuation's narrowing at all. This
 * drives one raw {@code table_function_plan} request carrying both fields at
 * once and asserts the author-facing {@link PlanRequest} received the merge.
 */
final class RefinedFiltersMergeTest {

    /** Records every {@link PlanRequest} it's called with; answers one dummy split so the RPC succeeds. */
    static final class RecordingPlanFunction implements TableFunction {

        final List<PlanRequest> observed = new ArrayList<>();

        @Override public String name() { return "recording_plan"; }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe("Records the PlanRequest it receives").withSplits();
        }

        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(ArgSpec.named("n", Schemas.INT64, "0"));
        }

        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(
                    SchemaUtil.serializeSchema(Schemas.of(Schemas.nullable("n", Schemas.INT64))));
        }

        @Override public PlanResult plan(TableBindParams params, PlanRequest request) {
            observed.add(request);
            return PlanResult.of(List.of(new ScanSplit(new byte[]{1}, new byte[0], 1L, true, 8L,
                    null, null, null, null, null)));
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            throw new UnsupportedOperationException("not redeemed in this test");
        }
    }

    @Test
    @Timeout(30)
    void refinedFiltersMergeWithPushdownFiltersOnTheSamePlanCall() throws Exception {
        RecordingPlanFunction fn = new RecordingPlanFunction();
        Worker worker = Worker.builder()
                .catalogName("testcat")
                .defaultSchema("main")
                .registerTable(fn);

        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker)) {
            VgiService vgi = h.client();
            byte[] handle = vgi.catalog_attach(
                    CatalogAttachRequest.of("testcat", null, null, null), null)
                    .attach_opaque_data();

            BindRequest bindRequest = new BindRequest(
                    "recording_plan",
                    ArgumentsEncoder.builder().named("n", 10L).encode(),
                    "TABLE", null, null, null, handle, null, false,
                    null, null, null, null, "main");
            BindResponse bound = vgi.bind(bindRequest, null);
            byte[] serializedBindCall = RecordCodec.serializeToBytes(bindRequest);

            ProjectedColumns cols = ProjectedColumns.of(List.of("n"));
            byte[] staticFilter = PushdownFiltersEncoder.builder()
                    .filter(cols.column("n"), FilterPredicate.gt(5L))
                    .encode().pushdownFilters();
            byte[] refined = PushdownFiltersEncoder.builder()
                    .filter(cols.column("n"), FilterPredicate.lt(100L))
                    .encode().pushdownFilters();

            TableFunctionPlanRequest planReq = new TableFunctionPlanRequest(
                    serializedBindCall, bound.opaque_data(),
                    null,               // projection_ids
                    staticFilter,
                    null,               // join_keys
                    null,               // row_limit
                    null, null, null,   // target_split_bytes, min_splits, max_splits_per_response
                    null,               // cursor — first call
                    refined,
                    false,              // filters_complete
                    null, null,         // start/end position
                    null, null, null, null,
                    null, null);
            vgi.table_function_plan(RecordCodec.serializeToBytes(planReq), null);

            assertEquals(1, fn.observed.size());
            PlanRequest received = fn.observed.get(0);
            assertNotNull(received.pushdownFilters(), "both wire fields present must decode to a non-null filter set");
            assertEquals(2, received.pushdownFilters().filters().size(),
                    "pushdown_filters' one filter and refined_filters' one filter must both reach plan()");
            String rendered = received.pushdownFilters().formatInline();
            assertTrue(rendered.contains("5"), "the static (pushdown_filters) fragment must be present: " + rendered);
            assertTrue(rendered.contains("100"), "the refined (refined_filters) fragment must be present: " + rendered);
            assertFalse(received.filtersComplete(), "filters_complete=false on the wire must reach plan() too");
        }
    }
}
