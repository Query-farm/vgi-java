// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Decode a 1-row IPC stream into {@code name → byte[]} for the requested
 * VarBinary columns. Per-cell nulls are simply omitted from the returned
 * map.
 *
 * <p>{@code null} return is reserved for the legitimate "no data" cases:
 * empty/null byte array, no batch in the stream, or a batch with zero
 * rows. Parse errors propagate as {@link IllegalStateException} so they
 * surface in the RPC response rather than being silently squashed into
 * a benign-looking empty result.</p>
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
                // Accept BOTH binary widths. A blind cast to VarBinaryVector
                // threw ClassCastException the moment a peer sent a LargeBinary
                // column — which is not a hypothetical: the two are
                // interchangeable on the wire for these fields, and which one a
                // given SDK emits is not something this reader gets to choose.
                org.apache.arrow.vector.FieldVector vec = root.getVector(name);
                if (vec == null || vec.isNull(0)) continue;
                if (vec instanceof VarBinaryVector v) {
                    out.put(name, v.get(0));
                } else if (vec instanceof org.apache.arrow.vector.LargeVarBinaryVector v) {
                    out.put(name, v.get(0));
                }
            }
            return out;
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("IpcUnpacker.unpack failed: " + e.getMessage(), e);
        }
    }

    /**
     * Decode a handful of Int64 scalar columns from the same 1-row struct
     * {@link #unpack} reads binary columns from. Missing/null cells are
     * simply absent from the returned map — same tolerant contract as
     * {@link #unpack}.
     *
     * @param request the 1-row IPC stream bytes
     * @param fieldNames the Int64 columns to read
     * @return {@code name -> value} for every present, non-null cell, or
     *         {@code null} for the same "no data" cases {@link #unpack} returns
     *         {@code null} for
     */
    static Map<String, Long> unpackLongs(byte[] request, String... fieldNames) {
        if (request == null || request.length == 0) return null;
        try (ByteArrayInputStream in = new ByteArrayInputStream(request);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return null;
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (root.getRowCount() == 0) return null;
            Map<String, Long> out = new HashMap<>();
            for (String name : fieldNames) {
                FieldVector vec = root.getVector(name);
                if (vec == null || vec.isNull(0)) continue;
                if (vec instanceof BigIntVector v) {
                    out.put(name, v.get(0));
                }
            }
            return out;
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("IpcUnpacker.unpackLongs failed: " + e.getMessage(), e);
        }
    }

    /**
     * Decode one {@code list<int64>} column from the same 1-row struct
     * {@link #unpack} reads binary columns from.
     *
     * @param request the 1-row IPC stream bytes
     * @param fieldName the list column to read
     * @return the list's values, {@code null} if the cell itself is
     *         null/absent, or {@code null} for the same "no data" cases
     *         {@link #unpack} returns {@code null} for
     */
    static List<Long> unpackLongList(byte[] request, String fieldName) {
        if (request == null || request.length == 0) return null;
        try (ByteArrayInputStream in = new ByteArrayInputStream(request);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return null;
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (root.getRowCount() == 0) return null;
            FieldVector vec = root.getVector(fieldName);
            if (!(vec instanceof ListVector listVec) || listVec.isNull(0)) return null;
            BigIntVector items = (BigIntVector) listVec.getDataVector();
            int start = listVec.getElementStartIndex(0);
            int end = listVec.getElementEndIndex(0);
            List<Long> out = new ArrayList<>(end - start);
            for (int i = start; i < end; i++) {
                out.add(items.isNull(i) ? null : items.get(i));
            }
            return out;
        } catch (IOException | RuntimeException e) {
            throw new IllegalStateException("IpcUnpacker.unpackLongList failed: " + e.getMessage(), e);
        }
    }
}
