// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

import java.util.List;

/**
 * Generic items list wire response. Used by every catalog query that returns
 * zero or more serialised info records (schemas, tables, views, functions, etc.).
 *
 * <p>Mirrors {@code vgi.ItemsResponseWire} / {@code vgi.CatalogsResponseWire}.
 */
public record ItemsResponse(List<byte[]> items) implements ArrowSerializableRecord {

    public static ItemsResponse empty() {
        return new ItemsResponse(List.of());
    }
}
