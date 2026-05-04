// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record SchemaContentsFunctionsRequest(
        byte[] attach_id,
        String name,
        String type,
        byte[] transaction_id) implements ArrowSerializableRecord {}
