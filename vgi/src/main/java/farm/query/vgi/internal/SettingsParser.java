// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.SmallIntVector;
import org.apache.arrow.vector.TinyIntVector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;

import java.io.ByteArrayInputStream;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Parse a {@code BindRequest.settings} IPC blob into a name→value map.
 *
 * <p>Each column of the 1-row settings batch represents one declared setting;
 * the column name is the setting's name and the cell holds its current value.
 * Mirrors {@code vgi.BatchToSettingsMap} in vgi-go.
 */
public final class SettingsParser {

    private SettingsParser() {}

    /**
     * Parse a settings IPC blob into a setting-name → value map.
     *
     * @param data the 1-row settings IPC stream (may be {@code null}/empty)
     * @return an immutable name → value map; empty when no row / no non-null cells
     */
    public static Map<String, Object> parse(byte[] data) {
        if (data == null || data.length == 0) return Map.of();
        try (ByteArrayInputStream in = new ByteArrayInputStream(data);
             ArrowStreamReader reader = new ArrowStreamReader(in, Allocators.root())) {
            if (!reader.loadNextBatch()) return Map.of();
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            if (root.getRowCount() == 0) return Map.of();
            Map<String, Object> out = new LinkedHashMap<>();
            for (FieldVector v : root.getFieldVectors()) {
                if (v.isNull(0)) continue;
                Object value = readScalar(v);
                if (value != null) out.put(v.getName(), value);
            }
            return Map.copyOf(out);
        } catch (Exception e) {
            throw new RuntimeException("SettingsParser failed", e);
        }
    }

    private static Object readScalar(FieldVector v) {
        if (v instanceof BigIntVector b) return b.get(0);
        if (v instanceof IntVector i) return (long) i.get(0);
        if (v instanceof SmallIntVector s) return (long) s.get(0);
        if (v instanceof TinyIntVector t) return (long) t.get(0);
        if (v instanceof Float8Vector f) return f.get(0);
        if (v instanceof BitVector b) return b.get(0) != 0;
        if (v instanceof VarCharVector vc) {
            byte[] bytes = vc.get(0);
            return bytes == null ? null : new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
        }
        if (v instanceof VarBinaryVector vb) return vb.get(0);
        if (v instanceof StructVector struct) {
            Map<String, Object> nested = new LinkedHashMap<>();
            for (FieldVector child : struct.getChildrenFromFields()) {
                if (!child.isNull(0)) nested.put(child.getName(), readScalar(child));
            }
            return Map.copyOf(nested);
        }
        return null;
    }
}
