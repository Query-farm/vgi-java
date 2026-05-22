// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

public record TransactionBeginResponse(
        @Nullable byte[] transaction_opaque_data) implements ArrowSerializableRecord {}
