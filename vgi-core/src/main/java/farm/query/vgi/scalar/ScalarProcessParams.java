// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.scalar;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

/** Parameters passed to {@link ScalarFunction#process}. */
public record ScalarProcessParams(
        String functionName,
        Arguments arguments,
        Schema outputSchema,
        Map<String, Object> settings,
        byte[] secrets) {

    public ScalarProcessParams(String functionName, Arguments arguments, Schema outputSchema,
                                 Map<String, Object> settings) {
        this(functionName, arguments, outputSchema, settings, null);
    }
}
