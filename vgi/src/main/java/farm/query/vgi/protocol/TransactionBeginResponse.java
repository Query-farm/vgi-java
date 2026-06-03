// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Response from {@code catalog_transaction_begin}.
 *
 * @param transaction_opaque_data worker-private transaction state echoed back on
 *                                subsequent transactional RPCs, or {@code null}.
 */
public record TransactionBeginResponse(
        @Nullable byte[] transaction_opaque_data) implements ArrowSerializableRecord {}
