// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.tableinout;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/**
 * Per-execution inputs handed to {@link TableInOutFunction#createExchange} when
 * a stream begins: the bound arguments, the negotiated input and (possibly
 * projection-narrowed) output schemas, and the allocator the exchange must use.
 *
 * @param functionName the bound function's name as invoked in SQL.
 * @param arguments the resolved positional/named arguments.
 * @param inputSchema the Arrow schema of the input stream.
 * @param outputSchema the output schema, already narrowed to the projected
 *     columns when the function opts into projection pushdown.
 * @param settings the session settings in effect for this execution.
 * @param allocator the allocator the exchange must use for all output vectors.
 * @param storage the per-execution shared-state facade (shard-pinned to the
 *     attach) — mirrors vgi-python's {@code params.storage}.
 * @param secrets the resolved-secrets IPC blob delivered by the two-phase bind,
 *     or {@code null}/empty when no secret was resolved. Parse with
 *     {@link farm.query.vgi.Secrets#parse(byte[])}.
 * @param substreamId the stable client-minted id for this streaming
 *     table-in-out substream, identical across this substream's init / every
 *     exchange tick / finalize — the key a worker uses to find a substream's
 *     accumulated state when an HTTP load balancer spreads that substream's
 *     requests across backends. {@code null} for the serial path or an old
 *     client. Mirrors vgi-python's {@code ProcessParams.substream_id}.
 */
public record TableInOutInitParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Schema outputSchema,
        Map<String, Object> settings,
        BufferAllocator allocator,
        farm.query.vgi.storage.BoundStorage storage,
        byte[] secrets,
        byte[] substreamId) {

    /**
     * Compatibility constructor without a substream id (serial path).
     *
     * @param functionName the bound function's name as invoked in SQL.
     * @param arguments the resolved positional/named arguments.
     * @param inputSchema the Arrow schema of the input stream.
     * @param outputSchema the (possibly projection-narrowed) output schema.
     * @param settings the session settings in effect for this execution.
     * @param allocator the allocator the exchange must use for output vectors.
     * @param storage the per-execution shared-state facade.
     * @param secrets the resolved-secrets IPC blob, or {@code null}.
     */
    public TableInOutInitParams(
            String functionName, Arguments arguments, Schema inputSchema, Schema outputSchema,
            Map<String, Object> settings, BufferAllocator allocator,
            farm.query.vgi.storage.BoundStorage storage, byte[] secrets) {
        this(functionName, arguments, inputSchema, outputSchema, settings, allocator,
                storage, secrets, null);
    }
}
