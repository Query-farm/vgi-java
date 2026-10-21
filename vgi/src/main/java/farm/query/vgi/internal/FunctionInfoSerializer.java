// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import farm.query.vgi.MetadataCollections;
import farm.query.vgi.protocol.FunctionInfo;
import java.util.List;

/** A singleton uses the same dictionary-aware codec as catalog-wide collections. */
final class FunctionInfoSerializer {
    private FunctionInfoSerializer() {}

    // Dictionary values now come from the records rather than a prebuilt enum table.
    // The fixture may therefore advertise WEIRD without changing a process-wide table.
    static void enableBadEnumNullHandling() {}

    static byte[] serialize(FunctionInfo info) {
        validateArgumentMonotonicity(info);
        return MetadataCollections.encode(List.of(info), FunctionInfo.class);
    }

    private static void validateArgumentMonotonicity(FunctionInfo info) {
        List<String> values = info.argument_monotonicity();
        if (values == null) return;
        if (!"SCALAR".equalsIgnoreCase(info.function_type())) {
            throw new IllegalArgumentException("argument_monotonicity is only valid for scalar functions");
        }
        int expected = SchemaUtil.deserializeSchema(info.arguments()).getFields().size();
        if (values.size() != expected) {
            throw new IllegalArgumentException("argument_monotonicity has " + values.size()
                    + " entries, expected " + expected + " declaration slots");
        }
        var allowed = java.util.Set.of("UNKNOWN", "CONSTANT", "NON_DECREASING",
                "STRICTLY_INCREASING", "NON_INCREASING", "STRICTLY_DECREASING");
        for (int i = 0; i < values.size(); i++) {
            if (!allowed.contains(values.get(i))) {
                throw new IllegalArgumentException("argument_monotonicity[" + i
                        + "] has unknown value " + values.get(i));
            }
        }
    }

}
