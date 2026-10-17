// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;

/**
 * Wire DTO for VGI bind responses.
 *
 * <p>Mirrors {@code vgi.BindResponseWire} in vgi-go.</p>
 *
 * @param output_schema serialised Arrow {@code Schema} bytes (IPC stream)
 * @param opaque_data bind-time state passed through to init
 * @param lookup_secret_types secret types to resolve; non-empty triggers the second bind pass
 * @param lookup_scopes secret scopes, parallel to {@code lookup_secret_types}
 * @param lookup_names secret names, parallel to {@code lookup_secret_types}; when any of these
 *     lists is non-empty, DuckDB resolves the named secrets and re-issues bind with
 *     {@code resolved_secrets_provided=true}
 */
public record BindResponse(
        byte[] output_schema,
        @Nullable byte[] opaque_data,
        List<String> lookup_secret_types,
        List<String> lookup_scopes,
        List<String> lookup_names) implements ArrowSerializableRecord {

    /**
     * Reads a {@code null} list/map component as empty: those columns are
     * non-nullable on the wire, and the record codec would otherwise write a
     * null into them, which a strict client rejects.
     */
    public BindResponse {
        lookup_secret_types = lookup_secret_types == null ? List.of() : lookup_secret_types;
        lookup_scopes = lookup_scopes == null ? List.of() : lookup_scopes;
        lookup_names = lookup_names == null ? List.of() : lookup_names;
    }


    /**
     * Builds a response carrying only an output schema, with no opaque data and no secret lookup.
     *
     * @param outputSchema serialised Arrow {@code Schema} bytes (IPC stream)
     * @return a {@code BindResponse} with empty opaque data and empty secret-lookup lists
     */
    public static BindResponse forSchema(byte[] outputSchema) {
        return new BindResponse(outputSchema, new byte[0], List.of(), List.of(), List.of());
    }
}
