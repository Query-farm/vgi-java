// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record AggregateBindRequest(
        String function_name,
        byte[] arguments,
        byte[] input_schema,
        byte[] settings,
        byte[] secrets,
        byte[] attach_id) implements ArrowSerializableRecord {}
