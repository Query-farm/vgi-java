// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.Map;

/** Mirrors the C++ {@code SchemaInfoSchema}. */
public record SchemaInfo(
        @Nullable String comment,
        Map<String, String> tags,
        byte[] attach_id,
        String name) implements ArrowSerializableRecord {
}
