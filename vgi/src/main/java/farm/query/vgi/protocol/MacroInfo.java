// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowField;
import farm.query.vgirpc.schema.ArrowFieldType;
import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;
import java.util.List;
import java.util.Map;

/** A complete canonical macro overload family, with catalog identity and annotations. */
public record MacroInfo(
        @Nullable String comment,
        Map<String, String> tags,
        String name,
        List<String> schema_path,
        @ArrowField(ArrowFieldType.DICT_INT16_UTF8) String macro_type,
        List<MacroOverload> overloads) implements ArrowSerializableRecord {
    public MacroInfo {
        if (overloads == null || overloads.isEmpty()) throw new IllegalArgumentException("macro family must be nonempty");
        for (var overload : overloads) {
            if (!overload.schema_path().equals(schema_path) || !overload.macro_type().equalsIgnoreCase(macro_type))
                throw new IllegalArgumentException("macro overload kind and owner must equal parent macro");
            if (overload.parameters().stream().anyMatch(p -> p.default_expression() != null))
                throw new IllegalArgumentException("discovery macro defaults must be resolved values");
        }
        overloads = List.copyOf(overloads);
    }
}
