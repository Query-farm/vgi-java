// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

public record FunctionRequiredSecret(
        String secret_type,
        @Nullable String scope,
        @Nullable String secret_name) implements ArrowSerializableRecord {}
