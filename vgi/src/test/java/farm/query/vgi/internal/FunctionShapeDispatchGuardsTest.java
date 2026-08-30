// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.Worker;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Regression tests: calling a function via the wrong RPC method shape.
 *
 * <p>Found live against a real deployed worker: a caller invoked a
 * table-in-out / blended row-transform function via the plain-producer path
 * ({@code table_function()}, no input stream) instead of
 * {@code table_in_out_function(input=...)} — or the mirror image, a plain
 * producer called via {@code table_in_out_function()}. Neither direction
 * failed cleanly. Both sides were independently, locally correct: the server
 * only stops on the function calling {@code finish()} (a row-transform
 * function never does — it is designed to consume input rows that never
 * arrive when the wrong RPC method was used), and the client only stops when
 * the server quits sending a continuation token (which never happens either,
 * since the server-side handler for this function was never designed to
 * reach that state). The result was a silent, non-terminating hang, not an
 * error.
 *
 * <p>Root cause: {@code initTableInOut} silently substituted an empty input
 * {@link Schema} when the bind's {@code input_schema} was missing, which
 * disabled a later schema-conformance check that depended on having
 * something concrete to compare against.
 *
 * <p>These tests pin that both directions now fail immediately, with a clear
 * message naming the function and the correct call to use — mirrors
 * vgi-python's {@code tests/test_function_shape_mismatch.py} and
 * vgi-typescript's {@code shape-mismatch.test.ts}.
 */
class FunctionShapeDispatchGuardsTest {

    private static final Schema TABLE_OUTPUT_SCHEMA =
            new Schema(List.of(Schemas.nullable("n", Schemas.INT64)));

    /** Minimal table-in-out function: echoes its input schema as output. */
    private static final class ProbeTio extends PassthroughTIOFunction {
        private static final FunctionSpec SPEC =
                FunctionSpec.builder("tio_probe").description("shape-mismatch test probe").build();

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
            return new TableInOutExchangeState() {
                @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
                    // Never exercised: every test here fails before a stream
                    // is driven.
                    out.finish();
                }
            };
        }
    }

    /** Minimal plain table function: a zero-input producer. */
    private static final class ProbeTable implements TableFunction {
        private static final FunctionSpec SPEC =
                FunctionSpec.builder("table_probe").description("shape-mismatch test probe").build();
        private static final byte[] OUTPUT_SCHEMA_IPC = SchemaUtil.serializeSchema(TABLE_OUTPUT_SCHEMA);

        @Override public FunctionSpec spec() { return SPEC; }

        @Override public BindResponse onBind(TableBindParams params) {
            return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
        }

        @Override public TableProducerState createProducer(TableInitParams params) {
            return new TableProducerState() {
                @Override public void produceTick(OutputCollector out, CallContext ctx) {
                    // Never exercised: every test here fails before a stream
                    // is driven.
                    out.finish();
                }
            };
        }
    }

    private static VgiServiceImpl service(Worker w) {
        return new VgiServiceImpl(w, w.scalars(), w.tables(), w.tableInOuts(), w.aggregates());
    }

    private static BindRequest bindRequest(String functionName, String functionType, byte[] inputSchema) {
        return new BindRequest(functionName, null, functionType, inputSchema, null, null,
                null, null, false, null, null, null, null, null);
    }

    // -----------------------------------------------------------------------
    // Direction 1: a table-in-out function called with no input schema
    // (i.e. via table_function() instead of table_in_out_function(input=...)).
    // -----------------------------------------------------------------------

    @Test
    void bindTableInOutWithNoInputSchemaIsRejected() {
        VgiServiceImpl svc = service(Worker.builder().catalogName("probe").registerTableInOut(new ProbeTio()));
        BindRequest req = bindRequest("tio_probe", "TABLE", null);

        IllegalArgumentException e = assertThrows(IllegalArgumentException.class, () -> svc.bind(req, null));
        assertTrue(e.getMessage().contains("tio_probe"), e.getMessage());
        assertTrue(e.getMessage().contains("table_in_out_function"), e.getMessage());
        assertTrue(e.getMessage().contains("table_function()"), e.getMessage());
    }

    @Test
    void bindTableInOutWithAnInputSchemaIsAccepted() {
        // Sanity check: the guard fires only on a genuinely ABSENT input
        // schema, not on the presence of one -- an ordinary, correctly-shaped
        // table_in_out_function(input=...) call must keep working.
        VgiServiceImpl svc = service(Worker.builder().catalogName("probe").registerTableInOut(new ProbeTio()));
        byte[] inputSchema = SchemaUtil.serializeSchema(
                new Schema(List.of(Schemas.nullable("x", Schemas.INT64))));
        BindRequest req = bindRequest("tio_probe", "TABLE", inputSchema);

        BindResponse resp = assertDoesNotThrow(() -> svc.bind(req, null));
        assertEquals("x", SchemaUtil.deserializeSchema(resp.output_schema()).getFields().get(0).getName());
    }

    @Test
    void bindTableInOutWithAPresentButZeroColumnInputSchemaIsAccepted() {
        // NOT the same case as "missing": a present-but-empty schema is the
        // legitimate signal a blended/varargs row-transform function's
        // childless call site sends (e.g. a no-arg row_sum() call), and must
        // not be conflated with "the caller used the wrong RPC method".
        VgiServiceImpl svc = service(Worker.builder().catalogName("probe").registerTableInOut(new ProbeTio()));
        byte[] emptyButPresent = SchemaUtil.serializeSchema(new Schema(List.of()));
        BindRequest req = bindRequest("tio_probe", "TABLE", emptyButPresent);

        assertDoesNotThrow(() -> svc.bind(req, null));
    }

    // -----------------------------------------------------------------------
    // Direction 2 (mirror image): a plain table function called with a
    // table-in-out init phase set (i.e. via table_in_out_function() instead
    // of table_function()).
    // -----------------------------------------------------------------------

    @Test
    void initTableWithATableInOutPhaseIsRejected() {
        VgiServiceImpl svc = service(Worker.builder().catalogName("probe").registerTable(new ProbeTable()));
        BindResponse bindResp = svc.bind(bindRequest("table_probe", "TABLE", null), null);

        InitRequest initReq = new InitRequest(
                new byte[0],                                  // bind_call (unused -- cached bind below)
                SchemaUtil.serializeSchema(TABLE_OUTPUT_SCHEMA), // output_schema
                bindResp.opaque_data(),                       // bind_opaque_data
                null, null, null,
                "INPUT",                                      // phase -- the mismatch signal
                null, null, null, null, null, null, null, null, null, null, null, null);

        IllegalStateException e = assertThrows(IllegalStateException.class, () -> svc.init(initReq, null));
        assertTrue(e.getMessage().contains("table_probe"), e.getMessage());
        assertTrue(e.getMessage().contains("table_function()"), e.getMessage());
        assertTrue(e.getMessage().contains("table_in_out_function"), e.getMessage());
    }

    @Test
    void initTableWithNoPhaseIsAccepted() {
        // Sanity check: an ordinary table_function() call (phase == null)
        // must keep working.
        VgiServiceImpl svc = service(Worker.builder().catalogName("probe").registerTable(new ProbeTable()));
        BindResponse bindResp = svc.bind(bindRequest("table_probe", "TABLE", null), null);

        InitRequest initReq = new InitRequest(
                new byte[0],
                SchemaUtil.serializeSchema(TABLE_OUTPUT_SCHEMA),
                bindResp.opaque_data(),
                null, null, null,
                null,                                          // phase -- absent, as a real table_function() call sends
                null, null, null, null, null, null, null, null, null, null, null, null);

        assertDoesNotThrow(() -> svc.init(initReq, null));
    }
}
