// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.client;

import farm.query.vgi.client.ArgumentsEncoder;
import farm.query.vgi.client.SettingsEncoder;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.errors.Code;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The Java leg of {@link AbstractVgiHttpConformanceTest}: the shared assertions
 * run against vgi-java's own example worker over HTTP.
 *
 * <p>Always runs — it needs nothing beyond this build — so a green Python leg
 * and a green Java leg together are what prove agreement, and a red one alone
 * names the side that is wrong.
 *
 * <p>It also pins the gRPC-style {@code error_code} the SDK puts on its own
 * errors (vgi-rpc WIRE_PROTOCOL §8). That half is Java-only for now: the
 * assertions are decoded from the wire by the vgi-rpc client, exactly as the
 * DuckDB extension reads them.
 */
final class JavaWorkerHttpConformanceTest extends AbstractVgiHttpConformanceTest {

    private static EmbeddedJavaHttpWorker worker;

    @BeforeAll
    static void startWorker() throws Exception {
        worker = EmbeddedJavaHttpWorker.start();
    }

    @AfterAll
    static void stopWorker() {
        if (worker != null) worker.close();
    }

    @Override
    protected VgiHttpWorkerUnderTest worker() {
        return worker;
    }

    /** {@code SELECT example.main.double('abc')}: a scalar's type rejection is the caller's fault. */
    @Test
    void scalarTypeRejectionIsInvalidArgument() {
        Schema input = new Schema(List.of(Field.nullable("value", new ArrowType.Utf8())));
        BindRequest bind = new BindRequest(
                "double", ArgumentsEncoder.builder().encode(), "SCALAR",
                SchemaUtil.serializeSchema(input), SettingsEncoder.builder().encode(), null,
                handle, null, false, null, null, null, null, "main");
        RpcError e = assertThrows(RpcError.class, () -> vgi.bind(bind, null));
        assertEquals(Code.INVALID_ARGUMENT.name(), e.errorCode(), e.errorMessage());
    }

    /** {@code sequence(10, batch_size := 0)}: a failed argument constraint is the caller's fault. */
    @Test
    void argumentConstraintIsInvalidArgument() {
        byte[] args = ArgumentsEncoder.builder().positional(10L).named("batch_size", 0L).encode();
        RpcError e = assertThrows(RpcError.class, () -> vgi.bind(tableBind("sequence", args), null));
        assertEquals(Code.INVALID_ARGUMENT.name(), e.errorCode(), e.errorMessage());
        assertTrue(e.errorMessage().contains("must be >= 1"), e.errorMessage());
    }

    /** Binding a function the worker does not serve is a failed lookup. */
    @Test
    void unknownFunctionIsNotFound() {
        RpcError e = assertThrows(RpcError.class, () -> vgi.bind(
                tableBind("no_such_function_at_all", ArgumentsEncoder.positionalArgs(1L)), null));
        assertEquals(Code.NOT_FOUND.name(), e.errorCode(), e.errorMessage());
    }

    /** DDL against the example catalog, which is not writable, is refused as a precondition. */
    @Test
    void ddlOnReadOnlyCatalogIsFailedPrecondition() {
        RpcError e = assertThrows(RpcError.class, () -> vgi.catalog_schema_create(
                handle, List.of("nope"), "error", null, java.util.Map.of(), null, null));
        assertEquals(Code.FAILED_PRECONDITION.name(), e.errorCode(), e.errorMessage());
        assertTrue(e.errorMessage().contains("read-only"), e.errorMessage());
    }
}
