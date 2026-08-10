// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi;

import farm.query.vgi.internal.AttachOptionSpecSerializer;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AttachOptionRequiredTest {

    /** An option that falls back to a value is always satisfiable without the
     *  caller, so declaring it required as well is a declaration bug. */
    @Test
    void requiredWithDefaultIsRejected() {
        var spec = AttachOptionSpec.of("region", "AWS region", new ArrowType.Utf8(), "us-east-1");
        assertThrows(IllegalArgumentException.class, () ->
                new AttachOptionSpec(spec.name(), spec.description(), spec.valueField(),
                        spec.defaultVector(), true));
    }

    @Test
    void requiredFactoryHasNoDefault() {
        var spec = AttachOptionSpec.required("api_key", "API key", new ArrowType.Utf8());
        assertTrue(spec.required());
        assertEquals(null, spec.defaultVector());
    }

    /** Writes the serialized specs so the cross-language check can decode them
     *  in Python. Skipped unless VGI_SPEC_DUMP_DIR is set. */
    @Test
    void dumpSpecsWhenRequested() throws Exception {
        String dir = System.getenv("VGI_SPEC_DUMP_DIR");
        if (dir == null) return;
        Files.write(Path.of(dir, "java_api_key.arrow"), AttachOptionSpecSerializer.serialize(
                AttachOptionSpec.required("api_key", "API key", new ArrowType.Utf8())));
        Files.write(Path.of(dir, "java_region.arrow"), AttachOptionSpecSerializer.serialize(
                AttachOptionSpec.of("region", "AWS region", new ArrowType.Utf8(), "us-east-1")));
    }
}
