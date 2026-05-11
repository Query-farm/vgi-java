// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.table;

import farm.query.vgi.function.Arguments;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Map;

public record TableBindParams(
        String functionName,
        Arguments arguments,
        Schema inputSchema,
        Map<String, Object> settings,
        byte[] secrets,
        boolean resolvedSecretsProvided) {

    public TableBindParams(String functionName, Arguments arguments, Schema inputSchema,
                            Map<String, Object> settings) {
        this(functionName, arguments, inputSchema, settings, null, false);
    }
}
