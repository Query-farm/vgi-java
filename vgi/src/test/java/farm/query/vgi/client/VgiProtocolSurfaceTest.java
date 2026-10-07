// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.Worker;
import farm.query.vgirpc.RpcError;
import farm.query.vgirpc.schema.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The {@code vgi.v2} surface is the reference's, method for method and schema for schema.
 *
 * <p>The protocol is the unit of optionality: every SDK registers every {@code vgi.v2} method
 * with exactly the reference's params, result and header schemas, so
 * {@code vgi_rpc.Reflection.v1} reports one protocol hash everywhere. A method this SDK does not
 * implement is still registered, and answers {@code UNIMPLEMENTED}.
 */
final class VgiProtocolSurfaceTest {

    /**
     * The canonical {@code vgi.v2} protocol hash of the reference (vgi-python 0.43.0, 72 methods).
     *
     * <p>This changes only when {@code vgi.v2}'s protocol version (currently 2.1.0) changes. If
     * this test fails without such a change, a method or a params/result/header schema (field
     * name, order, nullability or type) has drifted from the reference: fix the drift, do not
     * update the constant.
     */
    static final String REFERENCE_VGI_V2_HASH =
            "774cb80090d71ea76d09aa311b9cda4ca4c33c3bf72c43242eb6dc87b6f79ce5";

    /** The reference methods this SDK registers but does not implement. */
    private static final Set<String> UNIMPLEMENTED = new TreeSet<>(List.of(
            "aggregate_streaming_chunk", "aggregate_streaming_close", "aggregate_streaming_open",
            "aggregate_window", "aggregate_window_batch", "aggregate_window_destructor",
            "aggregate_window_init", "catalog_create", "catalog_drop", "catalog_index_create",
            "catalog_index_drop", "catalog_index_get", "catalog_macro_create", "catalog_macro_drop",
            "catalog_table_column_comment_set", "catalog_table_column_default_drop",
            "catalog_table_column_default_set", "catalog_table_column_rename",
            "catalog_table_column_type_change", "catalog_table_comment_set",
            "catalog_table_delete_function_get", "catalog_table_insert_function_get",
            "catalog_table_not_null_drop", "catalog_table_not_null_set", "catalog_table_rename",
            "catalog_table_update_function_get", "catalog_view_comment_set", "catalog_view_rename"));

    private static Worker worker() {
        return Worker.builder().catalogName("surface_probe");
    }

    @Test
    void vgiV2HashIsTheReference() {
        assertEquals(REFERENCE_VGI_V2_HASH, worker().rpcServer().protocolHash());
    }

    @Test
    @Timeout(60)
    void everyUnimplementedMethodAnswersUnimplementedOverTheWire() throws Exception {
        List<String> seen = new ArrayList<>();
        try (PipeWorkerHarness h = PipeWorkerHarness.start(worker())) {
            VgiService vgi = h.client();
            for (Method m : VgiService.class.getMethods()) {
                if (!UNIMPLEMENTED.contains(m.getName())) {
                    continue;
                }
                seen.add(m.getName());
                Object[] args = Arrays.stream(m.getParameters())
                        .map(VgiProtocolSurfaceTest::placeholder).toArray();
                InvocationTargetException thrown = org.junit.jupiter.api.Assertions.assertThrows(
                        InvocationTargetException.class, () -> m.invoke(vgi, args), m.getName());
                RpcError e = assertInstanceOf(RpcError.class, thrown.getCause(), m.getName());
                assertEquals("UNIMPLEMENTED", e.errorCode(), m.getName());
                assertEquals("method_not_implemented", e.errorKind(), m.getName());
                assertTrue(e.errorMessage().contains(m.getName() + " is not implemented by this worker"),
                        e.errorMessage());
            }
        }
        assertEquals(new ArrayList<>(UNIMPLEMENTED), seen.stream().sorted().toList());
    }

    /** A well-formed argument for a parameter; the stubs refuse before reading any of them. */
    private static Object placeholder(Parameter p) {
        Class<?> t = p.getType();
        if (t == farm.query.vgirpc.CallContext.class || p.isAnnotationPresent(Nullable.class)) {
            return null;
        }
        if (t == byte[].class) {
            return new byte[0];
        }
        if (t == String.class) {
            return "x";
        }
        if (t == boolean.class) {
            return false;
        }
        if (t == List.class) {
            return List.of("main");
        }
        throw new AssertionError("no placeholder for " + p);
    }
}
