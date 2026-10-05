// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgi.protocol.SchemaInfo;
import farm.query.vgi.protocol.ViewInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Catalog items encode their map columns in key order, so two equal maps built
 * in different orders give byte-identical items. {@code catalog_contents}
 * promises items byte-for-byte equal to the per-schema RPCs', and nothing may
 * hinge on a map's iteration order ({@code Map.of}'s changes per JVM).
 */
final class WireMapsDeterminismTest {

    private static <V> Map<String, V> ordered(Object... kv) {
        Map<String, V> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            @SuppressWarnings("unchecked") V v = (V) kv[i + 1];
            m.put((String) kv[i], v);
        }
        return m;
    }

    @Test
    void schemaInfoBytesDoNotDependOnMapOrder() {
        byte[] a = RecordCodec.serializeToBytes(new SchemaInfo("c", ordered("b", "2", "a", "1"), new byte[] {1},
                List.of("s"), ordered("table", 1L, "view", 0L, "index", 0L)));
        byte[] b = RecordCodec.serializeToBytes(new SchemaInfo("c", ordered("a", "1", "b", "2"), new byte[] {1},
                List.of("s"), ordered("index", 0L, "view", 0L, "table", 1L)));
        assertArrayEquals(a, b);
        assertEquals(List.of("a", "b"),
                List.copyOf(RecordCodec.deserializeFromBytes(a, SchemaInfo.class).tags().keySet()));
    }

    @Test
    void viewInfoBytesDoNotDependOnMapOrder() {
        byte[] a = RecordCodec.serializeToBytes(new ViewInfo(null, ordered("z", "1", "y", "2"), "v",
                List.of("s"), "SELECT 1", ordered("q", "x", "p", "y")));
        byte[] b = RecordCodec.serializeToBytes(new ViewInfo(null, ordered("y", "2", "z", "1"), "v",
                List.of("s"), "SELECT 1", ordered("p", "y", "q", "x")));
        assertArrayEquals(a, b);
    }

    @Test
    void handBuiltItemTagsAreWrittenInKeyOrder() {
        // MacroInfoSerializer, like the TableInfo / FunctionInfo / CopyFromFormatInfo
        // serialisers, writes tags through IpcStructBuilder.writeMap.
        byte[] a = MacroInfoSerializer.serialize(new MacroInfo(null, ordered("k2", "v2", "k1", "v1", "k3", null),
                "m", List.of("s"), "scalar", List.of("x"), null, "x", null));
        byte[] b = MacroInfoSerializer.serialize(new MacroInfo(null, ordered("k3", null, "k1", "v1", "k2", "v2"),
                "m", List.of("s"), "scalar", List.of("x"), null, "x", null));
        assertArrayEquals(a, b);
    }
}
