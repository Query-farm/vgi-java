// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Generic items list wire response. Used by every catalog query that returns
 * zero or more serialised info records (schemas, tables, views, functions, etc.).
 *
 * <p>Mirrors {@code vgi.ItemsResponseWire} / {@code vgi.CatalogsResponseWire}.
 *
 * @param items the serialised info records, one IPC-encoded blob per item.
 */
public record ItemsResponse(List<byte[]> items) implements ArrowSerializableRecord {

    /**
     * Returns an items response carrying no items.
     *
     * @return an empty response.
     */
    public static ItemsResponse empty() {
        return new ItemsResponse(List.of());
    }
}
