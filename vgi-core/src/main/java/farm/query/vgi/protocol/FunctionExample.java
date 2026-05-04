// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

public record FunctionExample(
        String sql,
        String description,
        @Nullable String expected_output) implements ArrowSerializableRecord {}
