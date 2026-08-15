// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.client;

import farm.query.vgi.VgiService;
import farm.query.vgi.internal.IpcStructBuilder;
import farm.query.vgi.protocol.BindRequest;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Build the request blobs for the two optimiser-facing table-function RPCs,
 * {@link VgiService#table_function_cardinality(byte[])} and
 * {@link VgiService#table_function_statistics(byte[])}.
 *
 * <p>Both take a <em>packed</em> outer {@code byte[]} rather than a normal
 * record: a one-row IPC batch of named binary fields, which the worker unpacks
 * with {@code IpcUnpacker}. Both expect the same two fields —
 * {@code bind_call} (the serialised {@link BindRequest} that produced the
 * binding) and {@code bind_opaque_data} (the handle the matching
 * {@code BindResponse} returned). A worker resolves the binding from the opaque
 * handle when it still has it and falls back to re-reading {@code bind_call},
 * so sending both is what makes the call work against a pooled or restarted
 * worker.
 *
 * <pre>{@code
 * BindResponse bound = vgi.bind(bindRequest, null);
 * byte[] req = TableFunctionRequests.forBind(bindRequest, bound.opaque_data());
 *
 * CardinalityResponse card = vgi.table_function_cardinality(req);
 * List<ColumnStatistics> stats =
 *         ColumnStatisticsDecoder.decode(vgi.table_function_statistics(req));
 * }</pre>
 *
 * <p>Cardinality comes back typed as {@code CardinalityResponse}; statistics
 * come back as raw IPC bytes for {@link ColumnStatisticsDecoder}.
 */
public final class TableFunctionRequests {

    private static final Schema PACKED_SCHEMA = new Schema(List.of(
            IpcStructBuilder.nullable("bind_call", IpcStructBuilder.BINARY),
            IpcStructBuilder.nullable("bind_opaque_data", IpcStructBuilder.BINARY)));

    private TableFunctionRequests() {}

    /**
     * Pack a cardinality/statistics request from the bind call and its handle.
     *
     * <p>The same blob serves both RPCs — they read identical fields — so a
     * client that asks for cardinality and statistics about one binding builds
     * it once.
     *
     * @param bindCall        the bind request that produced the binding
     * @param bindOpaqueData  the {@code BindResponse.opaque_data} handle, or
     *                        {@code null} if the caller has none
     * @return the packed request bytes
     */
    public static byte[] forBind(BindRequest bindCall, byte[] bindOpaqueData) {
        return forBind(bindCall == null ? null : RecordCodec.serializeToBytes(bindCall),
                bindOpaqueData);
    }

    /**
     * Pack a cardinality/statistics request from an already-serialised bind
     * call — e.g. the exact bytes the client also put in
     * {@code InitRequest.bind_call}.
     *
     * @param serialisedBindCall the serialised {@link BindRequest}, or {@code null}
     * @param bindOpaqueData     the {@code BindResponse.opaque_data} handle, or {@code null}
     * @return the packed request bytes
     */
    public static byte[] forBind(byte[] serialisedBindCall, byte[] bindOpaqueData) {
        return IpcStructBuilder.build(PACKED_SCHEMA, vectors -> {
            IpcStructBuilder.writeNullableVarBinary(vectors.get("bind_call"), serialisedBindCall);
            IpcStructBuilder.writeNullableVarBinary(vectors.get("bind_opaque_data"), bindOpaqueData);
        });
    }
}
