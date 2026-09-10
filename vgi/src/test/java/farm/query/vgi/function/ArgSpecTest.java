// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

import farm.query.vgi.types.Schemas;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ArgSpecTest {

    @Test
    void positionalWithDefaultIsSupported() {
        ArgSpec spec = new ArgSpec("station", 0, Schemas.UTF8, "doc",
                /*isConst=*/true, /*hasDefault=*/true, "asd",
                List.of(), false, false, false);
        assertEquals(0, spec.position());
        assertEquals(true, spec.hasDefault());
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
