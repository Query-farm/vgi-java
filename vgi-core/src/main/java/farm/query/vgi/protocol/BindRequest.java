// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record BindRequest(
        String function_name,
        byte[] arguments,
        String function_type,
        byte[] input_schema,
        byte[] settings,
        byte[] secrets,
        byte[] attach_id,
        byte[] transaction_id,
        boolean resolved_secrets_provided) implements ArrowSerializableRecord {}
