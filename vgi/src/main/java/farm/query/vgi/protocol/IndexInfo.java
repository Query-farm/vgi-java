// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;
import java.util.List;
import java.util.Map;

/** Resolved canonical index metadata, shared by items and catalog-wide collections. */
public record IndexInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        List<String> schema_path,
        byte[] definition,
        byte[] options) implements ArrowSerializableRecord {
    public IndexInfo {
        tags = WireMaps.sorted(tags == null ? Map.of() : tags);
        schema_path = schema_path == null ? List.of() : List.copyOf(schema_path);
    }
}
