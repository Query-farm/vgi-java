// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;
import java.util.List;
import java.util.Map;

/** Index metadata, shared by per-schema items and catalog-wide collections. */
public record IndexInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        List<String> schema_path,
        String table_name,
        String index_type,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String constraint_type,
        List<String> expressions,
        Map<String, String> options) implements ArrowSerializableRecord {}
