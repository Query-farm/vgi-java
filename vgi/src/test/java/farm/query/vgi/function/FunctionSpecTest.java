// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

import farm.query.vgi.types.Schemas;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FunctionSpecTest {

    @Test
    void builderAutoNumbersPositionalArgsInCallOrder() {
        FunctionSpec spec = FunctionSpec.builder("f")
                .description("d")
                .arg("a", Schemas.UTF8)
                .constArg("b", Schemas.INT64)
                .varargs("c", Schemas.FLOAT64)
                .build();

        List<ArgSpec> args = spec.argumentSpecs();
        assertEquals(0, args.get(0).position());
        assertEquals(1, args.get(1).position());
        assertEquals(2, args.get(2).position());
        assertEquals("d", spec.metadata().description());
    }

    @Test
    void namedArgConsumesNoPositionAndKeepsPositionalNumberingContiguous() {
        FunctionSpec spec = FunctionSpec.builder("f")
                .description("d")
                .arg("first", Schemas.UTF8)        // position 0
                .named("opt", Schemas.BOOL, "true") // position -1, no slot
                .arg("second", Schemas.INT64)       // position 1, not 2
                .build();

        ArgSpec named = spec.argumentSpecs().get(1);
        assertEquals(-1, named.position());
        assertTrue(named.hasDefault());
        assertEquals(0, spec.argumentSpecs().get(0).position());
        assertEquals(1, spec.argumentSpecs().get(2).position());
    }

    @Test
    void anyAndTableHelpersSetTheRightFlags() {
        FunctionSpec spec = FunctionSpec.builder("f")
                .description("d")
                .table("input")
                .any("v", TypeBoundPredicate.IS_ADDABLE)
                .build();

        assertTrue(spec.argumentSpecs().get(0).tableInput());
        assertTrue(spec.argumentSpecs().get(1).anyType());
        assertEquals(List.of(TypeBoundPredicate.IS_ADDABLE),
                spec.argumentSpecs().get(1).typeBound());
    }

    @Test
    void escapeHatchAppendsArgSpecVerbatim() {
        ArgSpec exotic = new ArgSpec("vals", 0, Schemas.FLOAT64, "", false, false, "",
                List.of(TypeBoundPredicate.IS_ADDABLE), /*varargs=*/true, /*anyType=*/true, false);
        FunctionSpec spec = FunctionSpec.builder("f").description("d").arg(exotic).build();

        assertSame(exotic, spec.argumentSpecs().get(0));
    }

    @Test
    void argumentSpecsAreImmutable() {
        FunctionSpec spec = FunctionSpec.builder("f").description("d").arg("a", Schemas.UTF8).build();
        assertThrows(UnsupportedOperationException.class,
                () -> spec.argumentSpecs().add(ArgSpec.positional("x", 1, Schemas.UTF8)));
    }

    @Test
    void trioDefaultsDelegateToSpec() {
        FunctionSpec backing = FunctionSpec.builder("my_fn")
                .description("desc")
                .arg("a", Schemas.UTF8)
                .build();
        FunctionDescriptor fn = new FunctionDescriptor() {
            @Override public FunctionSpec spec() { return backing; }
        };

        assertEquals("my_fn", fn.name());
        assertSame(backing.metadata(), fn.metadata());
        assertEquals(backing.argumentSpecs(), fn.argumentSpecs());
    }

    @Test
    void defaultSpecThrowsWhenNeitherSpecNorTrioOverridden() {
        // A descriptor that overrides nothing — calling any accessor surfaces
        // the "must override spec() or the trio" guard rather than recursing.
        FunctionDescriptor broken = new FunctionDescriptor() {};
        UnsupportedOperationException ex =
                assertThrows(UnsupportedOperationException.class, broken::name);
        assertTrue(ex.getMessage().contains("spec()"));
        assertFalse(ex.getMessage().isEmpty());
    }
}
