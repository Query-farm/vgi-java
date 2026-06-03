// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

/**
 * A documented usage example for a function, embedded in {@link FunctionInfo#examples}.
 *
 * @param sql             example SQL invoking the function.
 * @param description     human-readable explanation of the example.
 * @param expected_output expected result text, or {@code null} when not provided.
 */
public record FunctionExample(
        String sql,
        String description,
        @Nullable String expected_output) implements ArrowSerializableRecord {}
