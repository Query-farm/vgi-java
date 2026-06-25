// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MacroArgumentsSchemaTest {

    private static Field fieldByName(Schema schema, String name) {
        Field f = schema.findField(name);
        assertNotNull(f, "expected field '" + name + "' in schema");
        return f;
    }

    @Test
    void buildEmitsVgiDocForDocumentedParamsOnly() {
        List<String> params = List.of("val", "lo", "hi", "scale");
        Map<String, String> defaults = Map.of("lo", "0", "hi", "100");
        Map<String, String> docs = new java.util.LinkedHashMap<>();
        docs.put("val", "Value to clamp");
        docs.put("hi", "µ ≥ upper bound — note"); // unicode round-trip
        // "lo" and "scale" left undocumented.

        Schema schema = MacroArgumentsSchema.build(params, defaults, docs);

        // One field per parameter, in order.
        assertEquals(params, schema.getFields().stream().map(Field::getName).toList());

        // Documented field carries vgi_doc with the exact doc string.
        assertEquals("Value to clamp", fieldByName(schema, "val").getMetadata().get("vgi_doc"));

        // Unicode doc round-trips byte-for-byte (UTF-8).
        assertEquals("µ ≥ upper bound — note", fieldByName(schema, "hi").getMetadata().get("vgi_doc"));

        // Undocumented fields must NOT contain the vgi_doc key.
        Map<String, String> loMeta = fieldByName(schema, "lo").getMetadata();
        assertFalse(loMeta != null && loMeta.containsKey("vgi_doc"), "lo must not emit vgi_doc");
        Map<String, String> scaleMeta = fieldByName(schema, "scale").getMetadata();
        assertFalse(scaleMeta != null && scaleMeta.containsKey("vgi_doc"), "scale must not emit vgi_doc");

        // Field type: from default literal when known (lo/hi -> INT64), else Arrow null.
        assertEquals(new ArrowType.Int(64, true), fieldByName(schema, "lo").getType());
        assertEquals(new ArrowType.Int(64, true), fieldByName(schema, "hi").getType());
        assertTrue(fieldByName(schema, "val").getType() instanceof ArrowType.Null);
        assertTrue(fieldByName(schema, "scale").getType() instanceof ArrowType.Null);

        // Every field is nullable.
        for (Field f : schema.getFields()) {
            assertTrue(f.getFieldType().isNullable(), f.getName() + " should be nullable");
        }
    }

    @Test
    void ipcRoundTripThroughMacroInfoSerializer() {
        List<String> params = List.of("a", "b");
        Map<String, String> docs = Map.of("a", "first arg");

        byte[] argsSchemaIpc = MacroArgumentsSchema.toIpcBytes(params, Map.of(), docs);
        assertNotNull(argsSchemaIpc);

        farm.query.vgi.protocol.MacroInfo info = new farm.query.vgi.protocol.MacroInfo(
                "a comment", Map.of(), "m", "main", "scalar",
                params, new byte[0], "a + b", argsSchemaIpc);

        // Serialize the full MacroInfo (arguments_schema rides as the last field)
        // and confirm the embedded schema IPC bytes deserialize with vgi_doc intact.
        byte[] wire = MacroInfoSerializer.serialize(info);
        assertNotNull(wire);
        assertTrue(wire.length > 0);

        Schema decoded = SchemaUtil.deserializeSchema(argsSchemaIpc);
        Map<String, String> parsed = MacroArgumentsSchema.parameterDocsFromSchema(decoded);
        assertEquals(Map.of("a", "first arg"), parsed);
        // Field order preserved across IPC.
        assertEquals(params, decoded.getFields().stream().map(Field::getName).toList());
    }

    @Test
    void undocumentedMacroYieldsNoVgiDocKeys() {
        List<String> params = List.of("x", "y");
        Schema schema = MacroArgumentsSchema.build(params, Map.of(), Map.of());
        assertTrue(MacroArgumentsSchema.parameterDocsFromSchema(schema).isEmpty());
        for (Field f : schema.getFields()) {
            Map<String, String> meta = f.getMetadata();
            assertFalse(meta != null && meta.containsKey("vgi_doc"));
        }
    }

    @Test
    void macroRejectsDocForUnknownParameter() {
        try {
            new farm.query.vgi.catalog.Macro(
                    "main", "m", farm.query.vgi.catalog.MacroType.SCALAR,
                    List.of("x"), Map.of(), Map.of("nope", "bad"),
                    "x", "c", Map.of());
            org.junit.jupiter.api.Assertions.fail("expected IllegalArgumentException");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage().contains("nope"));
        }
    }
}
