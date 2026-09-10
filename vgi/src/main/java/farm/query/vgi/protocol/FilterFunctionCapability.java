// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;

/** Stable identity of one negotiated extension filter function. */
public record FilterFunctionCapability(
        String namespace,
        String name,
        @ArrowField(ArrowFieldType.UINT64) long version) implements ArrowSerializableRecord {

    public FilterFunctionCapability {
        FilterIdentityValidator.validate(namespace, name, version);
    }
}
