// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Wire DTO for VGI bind responses.
 *
 * <p>Mirrors {@code vgi.BindResponseWire} in vgi-go. Fields:
 * <ul>
 *   <li>{@code output_schema} — serialised Arrow Schema bytes (IPC stream).</li>
 *   <li>{@code opaque_data} — bind-time state passed through to init.</li>
 *   <li>{@code lookup_secret_types}/{@code lookup_scopes}/{@code lookup_names} —
 *       two-phase bind: when non-empty, DuckDB resolves the named secrets and
 *       re-issues bind with {@code resolved_secrets_provided=true}.</li>
 * </ul>
 */
public record BindResponse(
        byte[] output_schema,
        byte[] opaque_data,
        List<String> lookup_secret_types,
        List<String> lookup_scopes,
        List<String> lookup_names) implements ArrowSerializableRecord {

    public static BindResponse forSchema(byte[] outputSchema) {
        return new BindResponse(outputSchema, new byte[0], List.of(), List.of(), List.of());
    }
}
