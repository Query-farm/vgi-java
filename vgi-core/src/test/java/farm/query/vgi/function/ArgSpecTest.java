// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.function;

import farm.query.vgi.types.Schemas;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ArgSpecTest {

    @Test
    void positionalWithDefaultIsRejected() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> new ArgSpec("station", 0, Schemas.UTF8, "doc",
                        /*isConst=*/true, /*hasDefault=*/true, "asd",
                        List.of(), false, false, false));
        assertTrue(ex.getMessage().contains("station"));
        assertTrue(ex.getMessage().contains("position 0"));
    }

    @Test
    void positionalWithoutDefaultIsFine() {
        ArgSpec spec = ArgSpec.positional("station", 0, Schemas.UTF8);
        assertEquals("station", spec.name());
        assertEquals(0, spec.position());
        assertEquals(false, spec.hasDefault());
    }

    @Test
    void namedWithDefaultIsFine() {
        ArgSpec spec = ArgSpec.named("batch_size", Schemas.INT64, "1000");
        assertEquals(-1, spec.position());
        assertEquals(true, spec.hasDefault());
        assertEquals("1000", spec.defaultValue());
    }
}
