// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.function;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Where {@link ConstraintEnforcer} reads each const argument's value from.
 *
 * <p>Regression: named-only specs ({@link ArgSpec#named}, position -1) used to
 * consume positional slots, so a named argument was checked against another
 * argument's positional value (or skipped entirely when the slot was past the
 * end) and the positional argument after it was checked against the wrong slot.
 */
class ConstraintEnforcerTest {

    private static final ArrowType I64 = new ArrowType.Int(64, true);
    private static final ArrowType UTF8 = new ArrowType.Utf8();

    private static final ArgSpec LIMIT = ArgSpec.named("limit", I64, "10")
            .withConstraints(ArgSpec.Constraints.range(1, 100, null, null));
    private static final ArgSpec COUNT = ArgSpec.positional("count", 0, I64)
            .withConstraints(ArgSpec.Constraints.range(0, 1000, null, null));

    private static Arguments args(List<Object> positional, Map<String, Object> named) {
        return new Arguments(positional, named);
    }

    private static void rejects(String argName, Arguments a, List<ArgSpec> specs) {
        IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
                () -> ConstraintEnforcer.enforce(a, specs));
        assertTrue(e.getMessage().contains("'" + argName + "'"), e.getMessage());
    }

    @Test
    void namedOutOfRangeIsRejectedWhenDeclaredAfterPositional() {
        rejects("limit", args(List.of(3L), Map.of("limit", 5000L)), List.of(COUNT, LIMIT));
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(3L), Map.of("limit", 50L)), List.of(COUNT, LIMIT)));
    }

    @Test
    void namedOutOfRangeIsRejectedWhenDeclaredBeforePositional() {
        rejects("limit", args(List.of(3L), Map.of("limit", 5000L)), List.of(LIMIT, COUNT));
    }

    @Test
    void namedSpecDoesNotConsumeAPositionalSlot() {
        // count=500 is valid for count but out of range for limit: must not be
        // checked against limit's constraints.
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(500L), Map.of("limit", 5L)), List.of(LIMIT, COUNT)));
        // ...and the positional after the named spec still reads slot 0.
        rejects("count", args(List.of(5000L), Map.of("limit", 5L)), List.of(LIMIT, COUNT));
    }

    @Test
    void absentNamedSkipsConstraints() {
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(3L), Map.of()), List.of(LIMIT, COUNT)));
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(Arguments.empty(), List.of(LIMIT, COUNT)));
    }

    @Test
    void namedChoicesEnforced() {
        ArgSpec mode = ArgSpec.named("mode", UTF8, "fast")
                .withConstraints(ArgSpec.Constraints.choices(List.of("fast", "slow")));
        List<ArgSpec> specs = List.of(COUNT, mode);
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(1L), Map.of("mode", "slow")), specs));
        rejects("mode", args(List.of(1L), Map.of("mode", "medium")), specs);
    }

    @Test
    void namedGreaterLessEnforced() {
        ArgSpec ratio = ArgSpec.named("ratio", new ArrowType.FloatingPoint(
                        org.apache.arrow.vector.types.FloatingPointPrecision.DOUBLE), "0.5")
                .withConstraints(ArgSpec.Constraints.range(null, null, 0, 1));
        List<ArgSpec> specs = List.of(ratio, COUNT);
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(1L), Map.of("ratio", 0.25)), specs));
        rejects("ratio", args(List.of(1L), Map.of("ratio", 0.0)), specs);
        rejects("ratio", args(List.of(1L), Map.of("ratio", 1.0)), specs);
    }

    @Test
    void namedPatternEnforced() {
        ArgSpec region = ArgSpec.named("region", UTF8, "us-east-1")
                .withConstraints(ArgSpec.Constraints.pattern("^[a-z]+-[a-z]+-[0-9]$"));
        List<ArgSpec> specs = List.of(region);
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(), Map.of("region", "eu-west-2")), specs));
        rejects("region", args(List.of(), Map.of("region", "Not A Region")), specs);
    }

    @Test
    void patternIsAnUnanchoredSearch() {
        ArgSpec url = ArgSpec.positional("url", 0, UTF8)
                .withConstraints(ArgSpec.Constraints.pattern("^https?://"));
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of("http://127.0.0.1:8080/x"), Map.of()), List.of(url)));
        rejects("url", args(List.of("ftp://x"), Map.of()), List.of(url));

        ArgSpec word = ArgSpec.positional("text", 0, UTF8)
                .withConstraints(ArgSpec.Constraints.pattern("[0-9]+"));
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of("abc123def"), Map.of()), List.of(word)));
        rejects("text", args(List.of("abcdef"), Map.of()), List.of(word));

        ArgSpec anchored = ArgSpec.positional("code", 0, UTF8)
                .withConstraints(ArgSpec.Constraints.pattern("^[0-9]+$"));
        rejects("code", args(List.of("abc123def"), Map.of()), List.of(anchored));
    }

    @Test
    void positionalConstsAfterNonConstColumnsReadDenseConstSlots() {
        // A scalar compute(@Vector value, @Const precision): positional carries only
        // the const values, so precision (position 1) reads positional[0].
        ArgSpec value = new ArgSpec("value", 0, I64);
        ArgSpec precision = ArgSpec.positional("precision", 1, I64)
                .withConstraints(ArgSpec.Constraints.range(0, 10, null, null));
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(2L), Map.of()), List.of(value, precision)));
        rejects("precision", args(List.of(99L), Map.of()), List.of(value, precision));
    }

    @Test
    void everyVarargsValueIsChecked() {
        ArgSpec nums = ArgSpec.varargs("nums", 0, I64)
                .withConstraints(ArgSpec.Constraints.range(0, 9, null, null));
        assertDoesNotThrow(() -> ConstraintEnforcer.enforce(
                args(List.of(1L, 2L, 3L), Map.of()), List.of(nums)));
        rejects("nums", args(List.of(1L, 2L, 30L), Map.of()), List.of(nums));
    }
}
