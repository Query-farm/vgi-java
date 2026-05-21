// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Best-effort cleanup request for {@code table_buffering_destructor}. */
public record TableBufferingDestructorRequest(
        String function_name,
        byte[] execution_id,
        byte[] attach_opaque_data,
        byte[] transaction_id) implements ArrowSerializableRecord {}
