// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.protocol.BindRequest;
import farm.query.vgi.protocol.InitRequest;
import farm.query.vgi.protocol.Nested;

import java.util.ArrayList;
import java.util.List;

/**
 * Tests' InitRequest factory in the argument order the hand-written record had before the
 * generated one (protocol 3.4), with {@code bind_call} as raw bytes and int projection ids.
 */
public final class LegacyInit {
    private LegacyInit() {}

    /** Build an {@link InitRequest} from the pre-3.4 positional argument order. */
    @SuppressWarnings("checkstyle:ParameterNumber")
    public static InitRequest of(byte[] bindCall, byte[] outputSchema, byte[] bindOpaqueData,
            List<Integer> projectionIds, byte[] pushdownFilters, List<byte[]> joinKeys, String phase,
            byte[] executionId, byte[] initOpaqueData, String orderByColumnName, String orderByDirection,
            String orderByNullOrder, Long orderByLimit, Double tablesamplePercentage, Long tablesampleSeed,
            byte[] finalizeStateId, byte[] substreamId, List<byte[]> splitTokens, Long rowLimit) {
        List<Long> ids = null;
        if (projectionIds != null) {
            ids = new ArrayList<>(projectionIds.size());
            for (Integer i : projectionIds) ids.add(i == null ? null : (long) i);
        }
        return new InitRequest(bindCall == null ? null : Nested.fromBytes(bindCall, BindRequest.class),
                outputSchema, bindOpaqueData, ids, pushdownFilters, joinKeys, splitTokens, rowLimit, phase,
                finalizeStateId, executionId, initOpaqueData, substreamId, orderByColumnName,
                orderByDirection, orderByNullOrder, orderByLimit, tablesamplePercentage, tablesampleSeed);
    }
}
