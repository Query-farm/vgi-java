// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Combine-phase request for {@code table_buffering_combine} — once per query
 * after all input. Inner field order matches C++ {@code BuildTableBufferingCombineInner}.
 */
public record TableBufferingCombineRequest(
        String function_name,
        byte[] execution_id,
        List<byte[]> state_ids,
        byte[] attach_opaque_data,
        byte[] transaction_id) implements ArrowSerializableRecord {}
