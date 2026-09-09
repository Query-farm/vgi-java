// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.protocol;

import farm.query.vgirpc.schema.ArrowSerializableRecord;
import farm.query.vgirpc.schema.Nullable;

import java.util.List;

/**
 * Response describing the table function used to scan a catalog table.
 *
 * @param function_name       name of the scan function to invoke.
 * @param arguments           IPC-encoded bound arguments for the scan.
 * @param required_extensions DuckDB extensions that must be loaded to run the scan.
 * @param schema_path          schema containing the scan function, or {@code null}
 *                             for native or ambiguous functions.
 */
public record TableScanFunctionGetResponse(
        String function_name,
        byte[] arguments,
        List<String> required_extensions,
        @Nullable List<String> schema_path) implements ArrowSerializableRecord {
}
