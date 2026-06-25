// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArgumentSpecSerializerTest {

    private static Field fieldByName(Schema schema, String name) {
        Field f = schema.findField(name);
        assertNotNull(f, "expected field '" + name + "' in schema");
        return f;
    }

    @Test
    void toSchemaEmitsVgiDoc() {
        // Documented positional argument.
        ArgSpec documented = new ArgSpec("value", 0, Schemas.INT64,
                "Integer value to multiply",
                /*isConst=*/false, /*hasDefault=*/false, "",
                List.of(), /*varargs=*/false, /*anyType=*/false, /*tableInput=*/false);
        // Empty-doc positional argument (presence-only: vgi_doc must be absent).
        ArgSpec emptyDoc = new ArgSpec("factor", 1, Schemas.INT64,
                "",
                /*isConst=*/false, /*hasDefault=*/false, "",
                List.of(), /*varargs=*/false, /*anyType=*/false, /*tableInput=*/false);
        // Null-doc positional argument (presence-only: vgi_doc must be absent).
        ArgSpec nullDoc = new ArgSpec("offset", 2, Schemas.INT64,
                null,
                /*isConst=*/false, /*hasDefault=*/false, "",
                List.of(), /*varargs=*/false, /*anyType=*/false, /*tableInput=*/false);
        // Unicode doc to exercise UTF-8 round-tripping.
        ArgSpec unicodeDoc = new ArgSpec("threshold", 3, Schemas.INT64,
                "µ ≥ value — note",
                /*isConst=*/false, /*hasDefault=*/false, "",
                List.of(), /*varargs=*/false, /*anyType=*/false, /*tableInput=*/false);

        Schema schema = ArgumentSpecSerializer.toSchema(
                List.of(documented, emptyDoc, nullDoc, unicodeDoc));

        // Documented field carries vgi_doc with the exact doc string.
        Map<String, String> docMeta = fieldByName(schema, "value").getMetadata();
        assertEquals("Integer value to multiply", docMeta.get("vgi_doc"));

        // Empty-doc field must NOT contain the vgi_doc key.
        Map<String, String> emptyMeta = fieldByName(schema, "factor").getMetadata();
        assertFalse(emptyMeta.containsKey("vgi_doc"),
                "empty doc must not emit vgi_doc");

        // Null-doc field must NOT contain the vgi_doc key.
        Map<String, String> nullMeta = fieldByName(schema, "offset").getMetadata();
        assertFalse(nullMeta.containsKey("vgi_doc"),
                "null doc must not emit vgi_doc");

        // Unicode doc round-trips byte-for-byte (UTF-8).
        Map<String, String> unicodeMeta = fieldByName(schema, "threshold").getMetadata();
        assertTrue(unicodeMeta.containsKey("vgi_doc"));
        assertEquals("µ ≥ value — note", unicodeMeta.get("vgi_doc"));
    }
}
