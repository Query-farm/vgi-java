// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.function;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ParameterExtractorTest {

    private static ParameterExtractor extractor(List<Object> positional, Map<String, Object> named) {
        return ParameterExtractor.of(new Arguments(positional, named));
    }

    // ---------- required / orElse semantics ----------

    @Test
    void requiredThrowsWhenPositionalMissing() {
        ParameterExtractor p = extractor(List.of(), Map.of());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> p.positional(0, "count").asLong().required());
        assertEquals("count cannot be NULL", ex.getMessage());
    }

    @Test
    void requiredThrowsWhenNamedMissing() {
        ParameterExtractor p = extractor(List.of(), Map.of());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> p.named("batch_size").asLong().required());
        assertEquals("batch_size cannot be NULL", ex.getMessage());
    }

    @Test
    void requiredThrowsWhenNamedExplicitlyNull() {
        Map<String, Object> named = new HashMap<>();
        named.put("count", null);
        ParameterExtractor p = extractor(List.of(), named);
        assertThrows(IllegalArgumentException.class,
                () -> p.named("count").asLong().required());
    }

    @Test
    void orElseSubstitutesDefaultWhenMissing() {
        ParameterExtractor p = extractor(List.of(), Map.of());
        assertEquals(1000L, p.named("batch_size").asLong().orElse(1000L));
        assertEquals(1.5, p.named("inc").asDouble().orElse(1.5));
        assertEquals("first", p.named("layout").asString().orElse("first"));
        assertFalse(p.named("logging").asBool().orElse(false));
    }

    @Test
    void orElseSubstitutesDefaultWhenExplicitlyNull() {
        Map<String, Object> named = new HashMap<>();
        named.put("batch_size", null);
        ParameterExtractor p = extractor(List.of(), named);
        assertEquals(42L, p.named("batch_size").asLong().orElse(42L));
    }

    // ---------- notNull (speculative bind-time) ----------

    @Test
    void notNullTolerantsAbsence() {
        ParameterExtractor p = extractor(List.of(), Map.of());
        p.positional(0, "count").asLong().notNull();          // no throw
        p.named("batch_size").asLong().ge(1).notNull();       // no throw
        p.named("layout").asString().oneOf("a", "b").notNull();
    }

    @Test
    void notNullRejectsExplicitNull() {
        List<Object> pos = new java.util.ArrayList<>();
        pos.add(null);
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> ParameterExtractor.of(new Arguments(pos, Map.of()))
                        .positional(0, "count").asLong().notNull());
        assertEquals("count cannot be NULL", ex.getMessage());
    }

    @Test
    void notNullEnforcesRangeWhenPresent() {
        assertThrows(IllegalArgumentException.class,
                () -> extractor(List.of(0L), Map.of())
                        .positional(0, "count").asLong().ge(1).notNull());
    }

    // ---------- Long range checks ----------

    @Test
    void longGeRejectsBelowMin() {
        ParameterExtractor p = extractor(List.of(0L), Map.of());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> p.positional(0, "count").asLong().ge(1).required());
        assertEquals("count must be >= 1, got 0", ex.getMessage());
    }

    @Test
    void longLeRejectsAboveMax() {
        ParameterExtractor p = extractor(List.of(101L), Map.of());
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> p.positional(0, "n").asLong().le(100).required());
        assertEquals("n must be <= 100, got 101", ex.getMessage());
    }

    @Test
    void longBetweenIsInclusiveOnBothEnds() {
        // min boundary
        assertEquals(1L, extractor(List.of(1L), Map.of())
                .positional(0, "v").asLong().between(1, 10).required());
        // max boundary
        assertEquals(10L, extractor(List.of(10L), Map.of())
                .positional(0, "v").asLong().between(1, 10).required());
        // outside
        assertThrows(IllegalArgumentException.class, () -> extractor(List.of(0L), Map.of())
                .positional(0, "v").asLong().between(1, 10).required());
        assertThrows(IllegalArgumentException.class, () -> extractor(List.of(11L), Map.of())
                .positional(0, "v").asLong().between(1, 10).required());
    }

    @Test
    void longAcceptsIntegerOnTheWire() {
        // ArgumentsParser produces Number-but-not-always-Long values
        assertEquals(42L, extractor(List.of(Integer.valueOf(42)), Map.of())
                .positional(0, "v").asLong().required());
    }

    // ---------- Double constraints ----------

    @Test
    void doubleRejectsNaNByDefault() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> extractor(List.of(Double.NaN), Map.of())
                        .positional(0, "x").asDouble().required());
        assertEquals("x must be a finite number", ex.getMessage());
    }

    @Test
    void doubleRejectsInfinityByDefault() {
        assertThrows(IllegalArgumentException.class, () -> extractor(
                List.of(Double.POSITIVE_INFINITY), Map.of())
                .positional(0, "x").asDouble().required());
        assertThrows(IllegalArgumentException.class, () -> extractor(
                List.of(Double.NEGATIVE_INFINITY), Map.of())
                .positional(0, "x").asDouble().required());
    }

    @Test
    void doubleAllowNonFiniteAdmitsNaN() {
        double v = extractor(List.of(Double.NaN), Map.of())
                .positional(0, "x").asDouble().allowNonFinite().required();
        assertTrue(Double.isNaN(v));
    }

    @Test
    void doubleCoercesBigDecimal() {
        BigDecimal bd = new BigDecimal("0.25");
        assertEquals(0.25, extractor(List.of(bd), Map.of())
                .positional(0, "p").asDouble().between(0.0, 1.0).required());
    }

    @Test
    void doubleBetweenInclusiveBothEnds() {
        assertEquals(0.0, extractor(List.of(0.0), Map.of())
                .positional(0, "p").asDouble().between(0.0, 1.0).required());
        assertEquals(1.0, extractor(List.of(1.0), Map.of())
                .positional(0, "p").asDouble().between(0.0, 1.0).required());
    }

    // ---------- String constraints ----------

    @Test
    void stringOneOfRejectsUnknownValueWithCanonicalMessage() {
        IllegalArgumentException ex = assertThrows(IllegalArgumentException.class,
                () -> extractor(List.of(), Map.of("layout", "middling"))
                        .named("layout").asString().oneOf("first", "middle", "last").required());
        // matches RowIdSequenceFunction wording: "must be one of the allowed choices [first, middle, last], got '...'"
        assertEquals(
                "layout must be one of the allowed choices [first, middle, last], got 'middling'",
                ex.getMessage());
    }

    @Test
    void stringOneOfAdmitsListedValue() {
        assertEquals("middle", extractor(List.of(), Map.of("layout", "middle"))
                .named("layout").asString().oneOf("first", "middle", "last").required());
    }

    @Test
    void stringNonEmptyRejectsEmpty() {
        assertThrows(IllegalArgumentException.class,
                () -> extractor(List.of(""), Map.of())
                        .positional(0, "tag").asString().nonEmpty().required());
    }

    // ---------- Bool ----------

    @Test
    void boolReturnsValue() {
        assertTrue(extractor(List.of(), Map.of("logging", true))
                .named("logging").asBool().orElse(false));
        assertFalse(extractor(List.of(), Map.of("logging", false))
                .named("logging").asBool().orElse(true));
    }

    // ---------- varargs view ----------

    @Test
    void varargsFromEmptyWhenStartPastEnd() {
        ParameterExtractor p = extractor(List.of(1L, 2L), Map.of());
        assertSame(List.of(), p.varargsFrom(2));
        assertSame(List.of(), p.varargsFrom(5));
    }

    @Test
    void varargsFromReturnsTailSlice() {
        ParameterExtractor p = extractor(List.of(1L, 2L, 3L, 4L), Map.of());
        assertEquals(List.of(2L, 3L, 4L), p.varargsFrom(1));
        assertEquals(List.of(1L, 2L, 3L, 4L), p.varargsFrom(0));
    }

    // ---------- passthroughs ----------

    @Test
    void argumentsEscapeHatch() {
        Arguments a = new Arguments(List.of(1L), Map.of());
        assertSame(a, ParameterExtractor.of(a).arguments());
    }

    @Test
    void positionalCountIncludesPresentNulls() {
        List<Object> p = new java.util.ArrayList<>();
        p.add(null);
        p.add(1L);
        assertEquals(2, ParameterExtractor.of(new Arguments(p, Map.of())).positionalCount());
    }
}
