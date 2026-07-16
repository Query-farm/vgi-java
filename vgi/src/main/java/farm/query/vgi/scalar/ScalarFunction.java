// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import farm.query.vgi.function.FunctionDescriptor;
import farm.query.vgi.protocol.BindResponse;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;


/**
 * A scalar VGI function: 1:1 row mapping. Output row count must equal input row count.
 *
 * <p>Mirrors {@code vgi.ScalarFunction} in vgi-go.
 */
public interface ScalarFunction extends FunctionDescriptor {

    /**
     * Resolve output schema from bound argument types.
     *
     * @param params the bind-time arguments, input schema, and settings.
     * @return the bind response carrying the serialized output schema.
     */
    BindResponse onBind(ScalarBindParams params);

    /**
     * Transform an input batch into an output batch with the same row count and the
     * schema returned by {@link #onBind}. The caller owns the input root; the function
     * returns a freshly allocated output root that the caller will close.
     *
     * @param params the invocation arguments, output schema, and settings.
     * @param input the input batch (owned by the caller, do not close).
     * @param alloc allocator for the output root.
     * @return a freshly allocated output root the caller will close.
     */
    VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc);

    /**
     * Result-cache opt-in. When non-null, the returned {@code vgi.cache.*}
     * metadata rides every output batch's {@code custom_metadata} (NOT the
     * schema — the IPC stream fixes the schema at open), so the extension can
     * memoize the scalar's output per distinct input value. Only a pure,
     * deterministic scalar should advertise this — a non-pure scalar would
     * serve stale rows. Mirrors vgi-python's {@code ScalarFunction.CACHE_CONTROL}.
     *
     * @return the cache-control to advertise, or {@code null} (default) for a
     *     non-cacheable scalar.
     */
    default farm.query.vgi.cache.CacheControl cacheControl() { return null; }
}
