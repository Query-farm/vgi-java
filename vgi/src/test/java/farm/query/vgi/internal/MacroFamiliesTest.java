// Copyright 2026 Query Farm LLC - https://query.farm
package farm.query.vgi.internal;

import farm.query.vgi.catalog.Macro;
import farm.query.vgi.catalog.MacroType;
import farm.query.vgi.protocol.MacroInfo;
import farm.query.vgirpc.marshal.RecordCodec;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MacroFamiliesTest {
    @Test void resolvesSupportedLiteralsAndPreservesEmptyDocumentation() throws Exception {
        var info = MacroFamilies.fromDescriptor(new Macro("sales.archive", "m", MacroType.SCALAR,
            List.of("x", "fallback"), Map.of("fallback", "NULL"), Map.of("fallback", ""), "x", null, Map.of()));
        var restored = RecordCodec.deserializeFromBytes(MacroInfoSerializer.serialize(info), MacroInfo.class);
        assertEquals(List.of("sales.archive"), restored.schema_path());
        assertEquals(1, restored.overloads().size());
        var parameter = restored.overloads().getFirst().parameters().get(1);
        assertNull(parameter.declared_type());
        assertNull(parameter.default_expression());
        assertEquals("", parameter.documentation());
        try (var allocator = new RootAllocator(); var reader = new ArrowStreamReader(new ByteArrayInputStream(parameter.default_value()), allocator)) {
            assertTrue(reader.loadNextBatch());
            assertEquals(1, reader.getVectorSchemaRoot().getRowCount());
            assertTrue(reader.getVectorSchemaRoot().getVector(0).isNull(0));
        }
        assertEquals("can't", MacroDefaultsEncoder.valueForLiteral("'can''t'"));
        assertEquals(7L, MacroDefaultsEncoder.valueForLiteral("7"));
        assertEquals(true, MacroDefaultsEncoder.valueForLiteral("TRUE"));
        assertThrows(IllegalArgumentException.class, () -> MacroFamilies.fromDescriptor(new Macro("main", "m", MacroType.SCALAR,
            List.of("x"), Map.of("x", "random()"), "x", null)));
        assertThrows(IllegalArgumentException.class, () -> MacroFamilies.fromDescriptor(new Macro("main", "m", MacroType.SCALAR,
            List.of("x"), Map.of("missing", "1"), "x", null)));
    }
}
