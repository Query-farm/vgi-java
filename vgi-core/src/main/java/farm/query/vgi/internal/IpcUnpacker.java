// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.util.HashMap;
import java.util.Map;

/**
 * Decode a 1-row IPC stream into {@code name → byte[]} for the requested
 * VarBinary columns. Returns {@code null} on parse failure / empty batch
 * so callers can route to their own fallback. Per-cell nulls are simply
 * omitted from the returned map.
 *
 * <p>Used for the small handful of RPC requests that DuckDB sends as an
 * IPC stream of a 1-row struct of binary fields (cardinality_get,
 * dynamic_to_string).</p>
 */
final class IpcUnpacker {

    private IpcUnpacker() {}

    static Map<String, byte[]> unpack(byte[] request, String... fieldNames) {
        if (request == null || request.length == 0) return null;
        try (ByteArrayInputStream in = new ByteArrayInputStream(request);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return null;
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (root.getRowCount() == 0) return null;
            Map<String, byte[]> out = new HashMap<>();
            for (String name : fieldNames) {
                VarBinaryVector vec = (VarBinaryVector) root.getVector(name);
                if (vec != null && !vec.isNull(0)) out.put(name, vec.get(0));
            }
            return out;
        } catch (Exception e) {
            return null;
        }
    }
}
