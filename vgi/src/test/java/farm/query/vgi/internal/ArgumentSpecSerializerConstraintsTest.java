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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Emission of the per-argument constraint field metadata
 * ({@code vgi_default} / {@code vgi_choices} / {@code vgi_range} /
 * {@code vgi_pattern}) by {@link ArgumentSpecSerializer}. Byte-for-byte parity
 * with vgi-python's {@code argument_spec.py}; presence-only (a key is emitted
 * only when its constraint is present, never with an empty value).
 *
 * <p>All coverage lives here (JUnit) rather than in a DuckDB {@code .test}: the
 * integration suite drives a pinned published haybarn that lacks the new
 * {@code vgi_function_arguments()} columns, so a SQL test selecting them would
 * redden CI.
 */
class ArgumentSpecSerializerConstraintsTest {

    private static Map<String, String> metaFor(ArgSpec spec) {
        Schema schema = ArgumentSpecSerializer.toSchema(List.of(spec));
        Field f = schema.findField(spec.name());
        assertNotNull(f, "expected field '" + spec.name() + "'");
        return f.getMetadata();
    }

    /** Positional non-const argument carrying the given constraints. */
    private static ArgSpec positional(String name, ArgSpec.Constraints c) {
        return new ArgSpec(name, 0, Schemas.INT64).withConstraints(c);
    }

    // ---------- vgi_range: interval notation, all bound combinations ----------

    private static String rangeFor(Number ge, Number le, Number gt, Number lt) {
        ArgSpec spec = positional("x", ArgSpec.Constraints.range(ge, le, gt, lt));
        return metaFor(spec).get("vgi_range");
    }

    @Test
    void rangeInclusiveBothSides() {
        assertEquals("[0, 10]", rangeFor(0, 10, null, null));
    }

    @Test
    void rangeExclusiveLowerOpenUpper() {
        assertEquals("(0, +inf)", rangeFor(null, null, 0, null));
    }

    @Test
    void rangeInclusiveLowerExclusiveUpper() {
        assertEquals("[1, 10)", rangeFor(1, null, null, 10));
    }

    @Test
    void rangeOpenLowerInclusiveUpper() {
        assertEquals("(-inf, 100]", rangeFor(null, 100, null, null));
    }

    @Test
    void rangeExclusiveBothSides() {
        assertEquals("(0, 10)", rangeFor(null, null, 0, 10));
    }

    @Test
    void rangeExclusiveLowerPrecedesInclusiveLower() {
        // gt wins over ge on the low side; lt wins over le on the high side.
        assertEquals("(1, 9)", rangeFor(0, 10, 1, 9));
    }

    @Test
    void rangeIntegralDoublesPrintWithoutTrailingZero() {
        // Bounds arrive boxed as Double (the annotation path uses double); an
        // integral value must render as "0" / "10", not "0.0" / "10.0".
        assertEquals("[0, 10]", rangeFor(0.0d, 10.0d, null, null));
    }

    @Test
    void rangeFractionalDoublesPrintAsIs() {
        assertEquals("[0.5, 2.5]", rangeFor(0.5d, 2.5d, null, null));
    }

    @Test
    void rangeAbsentWhenNoBounds() {
        assertFalse(metaFor(positional("x", ArgSpec.Constraints.NONE)).containsKey("vgi_range"));
    }

    // ---------- vgi_choices: JSON array ----------

    @Test
    void choicesIntArrayJson() {
        ArgSpec spec = positional("x", ArgSpec.Constraints.choices(List.of(1, 2, 3)));
        assertEquals("[1,2,3]", metaFor(spec).get("vgi_choices"));
    }

    @Test
    void choicesStringArrayJson() {
        ArgSpec spec = positional("x", ArgSpec.Constraints.choices(List.of("a", "b")));
        assertEquals("[\"a\",\"b\"]", metaFor(spec).get("vgi_choices"));
    }

    @Test
    void choicesEmptyListStillEmitsEmptyArray() {
        // Mirrors Python: a non-null (even empty) choices set is present.
        ArgSpec spec = positional("x", ArgSpec.Constraints.choices(List.of()));
        assertEquals("[]", metaFor(spec).get("vgi_choices"));
    }

    @Test
    void choicesAbsentWhenNull() {
        assertFalse(metaFor(positional("x", ArgSpec.Constraints.NONE)).containsKey("vgi_choices"));
    }

    // ---------- vgi_pattern: raw regex ----------

    @Test
    void patternRawRegex() {
        ArgSpec spec = positional("x", ArgSpec.Constraints.pattern("^[a-z]+$"));
        assertEquals("^[a-z]+$", metaFor(spec).get("vgi_pattern"));
    }

    @Test
    void patternAbsentWhenNullOrEmpty() {
        assertFalse(metaFor(positional("x", ArgSpec.Constraints.NONE)).containsKey("vgi_pattern"));
        assertFalse(metaFor(positional("x", ArgSpec.Constraints.pattern("")))
                .containsKey("vgi_pattern"));
    }

    // ---------- vgi_default: JSON scalar, type-aware, presence-only ----------

    /** Named-only arg (position=-1) — the only shape allowed to carry a default. */
    private static ArgSpec named(String name, org.apache.arrow.vector.types.pojo.ArrowType type,
                                 String defaultLiteral) {
        return ArgSpec.named(name, type, defaultLiteral);
    }

    @Test
    void defaultIntegerUnquoted() {
        assertEquals("20", metaFor(named("history_size", Schemas.INT64, "20")).get("vgi_default"));
    }

    @Test
    void defaultFloatKeepsDecimal() {
        assertEquals("1.0", metaFor(named("increment", Schemas.FLOAT64, "1.0")).get("vgi_default"));
    }

    @Test
    void defaultBoolUnquoted() {
        assertEquals("false", metaFor(named("dup", Schemas.BOOL, "false")).get("vgi_default"));
    }

    @Test
    void defaultStringQuoted() {
        assertEquals("\"first\"", metaFor(named("layout", Schemas.UTF8, "first")).get("vgi_default"));
    }

    @Test
    void defaultNonNumericLiteralForNumericTypeFallsBackToString() {
        // Unparseable as INT → emitted as a JSON string rather than dropped.
        assertEquals("\"n/a\"", metaFor(named("k", Schemas.INT64, "n/a")).get("vgi_default"));
    }

    @Test
    void defaultAbsentWhenEmptyLiteral() {
        // Named args with an empty default literal ("no meaningful default") must
        // NOT emit vgi_default (presence-only).
        assertFalse(metaFor(named("ts", Schemas.INT64, "")).containsKey("vgi_default"));
    }

    @Test
    void defaultAbsentWhenNoDefault() {
        // Positional arg has no default concept at all.
        assertFalse(metaFor(positional("x", ArgSpec.Constraints.NONE)).containsKey("vgi_default"));
    }

    // ---------- combined + absence contract ----------

    @Test
    void unconstrainedArgEmitsNoConstraintKeys() {
        Map<String, String> meta = metaFor(positional("plain", ArgSpec.Constraints.NONE));
        assertFalse(meta.containsKey("vgi_default"));
        assertFalse(meta.containsKey("vgi_choices"));
        assertFalse(meta.containsKey("vgi_range"));
        assertFalse(meta.containsKey("vgi_pattern"));
    }

    @Test
    void multipleConstraintsCoexistOnOneField() {
        ArgSpec.Constraints c = new ArgSpec.Constraints(
                List.of("x", "y"), 0, 5, null, null, "^[xy]$");
        Map<String, String> meta = metaFor(positional("mode", c));
        assertEquals("[\"x\",\"y\"]", meta.get("vgi_choices"));
        assertEquals("[0, 5]", meta.get("vgi_range"));
        assertEquals("^[xy]$", meta.get("vgi_pattern"));
    }

    @Test
    void nullConstraintsNormalisesToNone() {
        // ArgSpec canonical constructor normalises a null Constraints → NONE, so
        // serialization never NPEs and emits no constraint keys.
        ArgSpec spec = new ArgSpec("x", 0, Schemas.INT64).withConstraints(null);
        assertEquals(ArgSpec.Constraints.NONE, spec.constraints());
        Map<String, String> meta = metaFor(spec);
        assertFalse(meta.containsKey("vgi_range"));
    }

    @Test
    void constraintsRecordIsEmptyReflectsPresence() {
        assertTrue(ArgSpec.Constraints.NONE.isEmpty());
        assertFalse(ArgSpec.Constraints.range(0, null, null, null).isEmpty());
        assertNull(ArgSpec.Constraints.NONE.pattern());
    }
}
