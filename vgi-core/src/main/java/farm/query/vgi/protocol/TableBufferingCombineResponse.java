// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/** Response from {@code table_buffering_combine} — the finalize partition keys. */
public record TableBufferingCombineResponse(
        List<byte[]> finalize_state_ids) implements ArrowSerializableRecord {}
