// Copyright 2025, 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.protocol;
import farm.query.vgirpc.schema.*;
import java.util.List;
/**
 * Pure optional scalar projection proposal; binary records retain explicit
 * context.
 */
public record TableFunctionProjectionRequest(
    byte[] bind_call, byte[] bind_result,
    @ArrowField(ArrowFieldType.LARGE_BINARY) List<byte[]> expressions,
    byte[] result_schema) implements ArrowSerializableRecord {}
