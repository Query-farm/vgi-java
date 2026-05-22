// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;

public record CatalogAttachRequest(
        String name,
        byte[] options,
        String data_version_spec,
        String implementation_version) implements ArrowSerializableRecord {}
