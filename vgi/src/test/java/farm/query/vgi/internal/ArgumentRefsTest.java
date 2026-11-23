// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import static org.junit.jupiter.api.Assertions.*;

import farm.query.vgi.LegacyInit;
import farm.query.vgi.protocol.*;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AuthContext;
import farm.query.vgirpc.errors.Code;
import farm.query.vgirpc.errors.StatusError;
import farm.query.vgirpc.marshal.RecordCodec;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

/**
 * Protocol 3.4 argument references and the bind envelope (vgi docs/large-values-3.4.md §2, §5a,
 * §3.3 rule 11).
 */
class ArgumentRefsTest {
    private static final String SHA = "ab".repeat(32);
    private static final Schema SOURCE = new Schema(List.of(Schemas.nullable("n", Schemas.INT32)));
    private static final byte[] SECRET = "SECRET-VALUE-0123456789-do-not-leak".getBytes(StandardCharsets.UTF_8);

    private static BindRequest call(byte[] arguments, ArgumentsLocation location, byte[] secrets) {
        return new BindRequest("probe", arguments, "table", null, null, secrets, null, null,
                secrets != null, null, null, null, null, List.of("main"), null, location);
    }

    @Test void exactlyOneOfArgumentsAndLocation() {
        var neither = assertThrows(StatusError.class, () -> BindArguments.validate(call(null, null, null)));
        assertEquals(BindArguments.INVALID_BIND_REQUEST_KIND, neither.errorKind());
        assertEquals(Code.INVALID_ARGUMENT, neither.errorCode());
        assertEquals(BindArguments.INVALID_BIND_REQUEST_MESSAGE, neither.getMessage());
        var both = assertThrows(StatusError.class, () -> BindArguments.validate(
                call(new byte[0], new ArgumentsLocation(new byte[] {1}, 10, SHA), null)));
        assertEquals(BindArguments.INVALID_BIND_REQUEST_KIND, both.errorKind());
        assertDoesNotThrow(() -> BindArguments.validate(call(new byte[0], null, null)));
        assertDoesNotThrow(() -> BindArguments.validate(call(null, new ArgumentsLocation(new byte[] {1}, 10, SHA), null)));
    }

    @Test void malformedLocationIsInvalidArgumentsRef() {
        for (var loc : List.of(new ArgumentsLocation(new byte[] {1}, 0, SHA),
                new ArgumentsLocation(new byte[] {1}, 5, SHA.toUpperCase(Locale.ROOT)),
                new ArgumentsLocation(new byte[] {1}, 5, "abc"),
                new ArgumentsLocation(new byte[0], 5, SHA))) {
            var e = assertThrows(StatusError.class, () -> BindArguments.validate(call(null, loc, null)));
            assertEquals(BindArguments.INVALID_ARGUMENTS_REF_KIND, e.errorKind());
            assertTrue(e.getMessage().startsWith("invalid_arguments_ref: "), e.getMessage());
        }
    }

    @Test void aLocationWithoutAContextIsRefused() {
        var e = assertThrows(StatusError.class, () ->
                BindArguments.resolveIpc(call(null, new ArgumentsLocation(new byte[] {1}, 10, SHA), null), null));
        assertEquals(BindArguments.INVALID_ARGUMENTS_REF_KIND, e.errorKind());
    }

    @Test void preparedStatementMessageIsExact() {
        assertEquals("this prepared statement's large argument has expired; prepare it again",
                BindArguments.MISSING_MESSAGE);
        assertEquals("arguments_ref_missing", BindArguments.MISSING_KIND);
    }

    @Test void fingerprintUsesSizeAndDigestNeverTheRef() {
        var a = call(null, new ArgumentsLocation(new byte[] {1, 2, 3}, 1234, SHA), null);
        var b = call(null, new ArgumentsLocation(new byte[] {9, 9}, 1234, SHA), null);
        assertArrayEquals(BindArguments.fingerprintInput(a), BindArguments.fingerprintInput(b));
        byte[] in = BindArguments.fingerprintInput(a);
        assertEquals(1 + 8 + 32, in.length);
        assertEquals(1, in[0]);
        var c = call(null, new ArgumentsLocation(new byte[] {1, 2, 3}, 1235, SHA), null);
        assertFalse(Arrays.equals(in, BindArguments.fingerprintInput(c)));
        byte[] inline = BindArguments.fingerprintInput(call(new byte[] {7, 8}, null, null));
        assertArrayEquals(new byte[] {0, 2, 0, 0, 0, 0, 0, 0, 0, 7, 8}, inline);
    }

    @Test void argumentsRefHeaderValueFromEnv() {
        assertEquals(4L * 1024 * 1024, BindArguments.bytesFromEnv(Map.of()));
        assertEquals(1234L, BindArguments.bytesFromEnv(Map.of(BindArguments.BYTES_ENV, "1234")));
        assertThrows(IllegalArgumentException.class, () -> BindArguments.bytesFromEnv(Map.of(BindArguments.BYTES_ENV, "0")));
        assertThrows(IllegalArgumentException.class, () -> BindArguments.bytesFromEnv(Map.of(BindArguments.BYTES_ENV, "-1")));
        assertTrue(BindArguments.disabled(Map.of(BindArguments.DISABLE_ENV, "1")));
        assertFalse(BindArguments.disabled(Map.of()));
    }

    // --- the projection / repeatability bind envelope ------------------------------------

    private static final class Offered extends farm.query.vgi.table.SimpleTableFunction {
        @Override public farm.query.vgi.function.FunctionSpec spec() {
            return farm.query.vgi.function.FunctionSpec.builder("probe").description("envelope probe").build();
        }
        @Override protected Schema outputSchema() { return SOURCE; }
        @Override public BindResponse onBind(farm.query.vgi.table.TableBindParams params) {
            return new BindResponse(SchemaUtil.serializeSchema(SOURCE), new byte[] {42}, List.of(), List.of(),
                    List.of(), List.of(ProjectionExpressions.PROFILE), BindRepeatability.STABLE_WITHIN_QUERY);
        }
        @Override public farm.query.vgi.table.TableProducerState createProducer(farm.query.vgi.table.TableInitParams params) {
            return new farm.query.vgi.table.TableProducerState() {
                @Override public void produceTick(farm.query.vgirpc.OutputCollector out, farm.query.vgirpc.CallContext ctx) { out.finish(); }
            };
        }
    }

    private static VgiServiceImpl service(boolean sealed) {
        var worker = farm.query.vgi.Worker.builder().catalogName("probe").registerTable(new Offered());
        return new VgiServiceImpl(worker, worker.scalars(), worker.tables(), worker.tableInOuts(), worker.aggregates(),
                sealed);
    }

    private static boolean contains(byte[] haystack, byte[] needle) {
        outer:
        for (int i = 0; i + needle.length <= haystack.length; i++) {
            for (int j = 0; j < needle.length; j++) if (haystack[i + j] != needle[j]) continue outer;
            return true;
        }
        return false;
    }

    @Test void secretsNeverAppearInTheClearInTheBindResponseOrPlan() {
        for (boolean sealed : new boolean[] {false, true}) {
            var svc = service(sealed);
            var request = call(new byte[0], null, SECRET);
            BindResponse bound = svc.bind(request, null);
            assertNotNull(bound.opaque_data());
            // The envelope embeds the bind call (secrets included) and must be sealed on every
            // transport: bind_opaque_data leaves the worker and lands in serialized plans.
            assertFalse(contains(bound.opaque_data(), SECRET), "bind_opaque_data carries the secret in the clear");
            assertFalse(contains(RecordCodec.serializeToBytes(bound), SECRET), "BindResponse carries the secret in the clear");
            // An equivalent rebind gives the same value (STABLE_WITHIN_QUERY / portable state).
            assertArrayEquals(bound.opaque_data(), svc.bind(request, null).opaque_data());
            // The init, which re-opens the envelope, still works.
            assertDoesNotThrow(() -> svc.init(LegacyInit.of(RecordCodec.serializeToBytes(request),
                    bound.output_schema(), bound.opaque_data(), null, null, null, null, null, null, null, null,
                    null, null, null, null, null, null, null, null), null));
            // The plan response (the split tokens a serialized plan carries) never holds it either.
            var plan = new TableFunctionPlanRequest(Nested.of(request), bound.opaque_data(), null, null, null,
                    null, null, null, null, null, null, true, null, null, null, null, null, null, null, null);
            var planned = svc.table_function_plan(RecordCodec.serializeToBytes(plan), null);
            assertFalse(contains(RecordCodec.serializeToBytes(planned), SECRET), "plan response carries the secret");
        }
    }

    @Test void theEnvelopeDoesNotOpenForAnotherPrincipal() {
        var svc = service(true);
        var request = call(new byte[0], null, SECRET);
        var alice = new AuthContext("test", true, "alice", Map.of());
        var bob = new AuthContext("test", true, "bob", Map.of());
        var seal = StoragePayloadSeal.forKey(new byte[32]);
        BindResponse result = BindResponse.forSchema(SchemaUtil.serializeSchema(SOURCE));
        byte[] env = ProjectionExpressions.envelope(result, RecordCodec.serializeToBytes(request), seal, alice);
        assertNotNull(ProjectionExpressions.open(env, Nested.of(request), seal, alice));
        assertThrows(IllegalArgumentException.class, () -> ProjectionExpressions.open(env, Nested.of(request), seal, bob));
        assertNotNull(svc);
    }

    @Test void reEncodedBindCallEqualsTheReceivedBytesAfterOpening() {
        // Rule 11: opened sealed values live beside the Nested wrapper, never in it. Drive an init
        // whose bind_call carries a sealed attach, then re-encode the request.
        var svc = service(true);
        byte[] attach = svc.catalog_attach(CatalogAttachRequest.of("probe", null, null, null), null).attach_opaque_data();
        var request = new BindRequest("probe", new byte[0], "table", null, null, SECRET, attach, null, true,
                null, null, null, null, List.of("main"), null, null);
        byte[] received = RecordCodec.serializeToBytes(request);
        var bound = svc.bind(request, null);
        InitRequest init = LegacyInit.of(received, bound.output_schema(), bound.opaque_data(),
                null, null, null, null, null, null, null, null, null, null, null, null, null, null, null, null);
        byte[] wire = RecordCodec.serializeToBytes(init);
        InitRequest decoded = RecordCodec.deserializeFromBytes(wire, InitRequest.class);
        assertDoesNotThrow(() -> svc.init(decoded, null));
        assertArrayEquals(received, decoded.bind_call().rawBinary());
        InitRequest reencoded = RecordCodec.deserializeFromBytes(RecordCodec.serializeToBytes(decoded), InitRequest.class);
        assertArrayEquals(received, reencoded.bind_call().rawBinary());
    }
}
