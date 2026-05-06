// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.Map;

/**
 * Mirrors the C++ {@code ViewInfoSchema}.
 */
public record ViewInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        String schema_name,
        String definition) implements ArrowSerializableRecord {
}
