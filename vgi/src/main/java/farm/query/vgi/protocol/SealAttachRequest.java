// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * Request for {@code vgi.attach_tickets.v1} {@code seal_attach}
 * ({@code docs/protocol/vgi-attach-tickets.md} §5.2). Component order is column order and
 * {@code @Nullable} is wire nullability, matching vgi-python's {@code SealAttachRequest}.
 *
 * @param catalog_name the catalog the caller attached
 * @param options the options the caller attached with, secret ones included: a one-row record as
 *        Arrow IPC, exactly as {@code CatalogAttachRequest.options}; {@code null} for none
 * @param data_version_spec as given at ATTACH; {@code ""} for none
 * @param implementation_version as given at ATTACH; {@code ""} for none
 * @param ttl_seconds requested lifetime; {@code 0} asks for as long as the worker allows
 */
public record SealAttachRequest(
        String catalog_name,
        @Nullable byte[] options,
        String data_version_spec,
        String implementation_version,
        long ttl_seconds) implements ArrowSerializableRecord {
}
