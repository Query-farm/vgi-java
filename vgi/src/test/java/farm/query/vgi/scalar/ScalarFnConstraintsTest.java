// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.internal.ArgumentSpecSerializer;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * The discovery constraints declared on {@link Const} / {@link Vector}
 * annotations thread through {@code ScalarFn}'s auto-derived {@link ArgSpec}s and
 * onto the serialized Arrow field metadata — the Java analog of vgi-python's
 * {@code scalar_function.py} threading {@code ConstParam(ge=..., le=...)} into
 * {@code Arg}. Mirrors the reference fixture's {@code format_number} overload
 * ({@code precision} const param declared {@code ge=0, le=10}).
 */
class ScalarFnConstraintsTest {

    /** {@code format_number(precision, value)} — precision bounded to [0, 10]. */
    public static final class FormatNumber extends ScalarFn {
        @Override public String name() { return "format_number"; }

        /**
         * @param precision decimal places, bounded [0, 10].
         * @param value     value to format.
         * @param result    output vector.
         */
        public void compute(
                @Const(doc = "Number of decimal places", ge = 0, le = 10) long precision,
                @Vector(doc = "Number to format") Float8Vector value,
                VarCharVector result) {
            int rows = value.getValueCount();
            String fmt = "%." + precision + "f";
            for (int i = 0; i < rows; i++) {
                if (value.isNull(i)) {
                    result.setNull(i);
                } else {
                    result.setSafe(i, String.format(fmt, value.get(i)).getBytes());
                }
            }
        }
    }

    private static ArgSpec argByName(List<ArgSpec> specs, String name) {
        return specs.stream().filter(s -> s.name().equals(name)).findFirst().orElseThrow();
    }

    @Test
    void constParamBoundsThreadIntoArgSpecConstraints() {
        List<ArgSpec> specs = new FormatNumber().argumentSpecs();

        ArgSpec precision = argByName(specs, "precision");
        ArgSpec.Constraints c = precision.constraints();
        assertEquals(0.0d, c.ge());
        assertEquals(10.0d, c.le());
        assertNull(c.gt());
        assertNull(c.lt());
        assertNull(c.choices());
        assertNull(c.pattern());

        // The unbounded value column carries no constraints.
        assertEquals(ArgSpec.Constraints.NONE, argByName(specs, "value").constraints());
    }

    @Test
    void boundsSurfaceAsVgiRangeMetadata() {
        List<ArgSpec> specs = new FormatNumber().argumentSpecs();
        Schema schema = ArgumentSpecSerializer.toSchema(specs);

        assertEquals("[0, 10]", schema.findField("precision").getMetadata().get("vgi_range"));
        // value is unconstrained → no vgi_range key.
        assertNull(schema.findField("value").getMetadata().get("vgi_range"));
    }

    @Test
    void constBoundViolationRejectedAtBind() {
        FormatNumber fn = new FormatNumber();
        // In-range precision binds cleanly (null inputSchema → type-bound check skipped).
        fn.onBind(new ScalarBindParams(
                "format_number", new Arguments(List.of(2L), Map.of()), null, Map.of()));
        // A const value outside [0, 10] is rejected at bind, not silently used.
        assertThrows(IllegalArgumentException.class, () -> fn.onBind(new ScalarBindParams(
                "format_number", new Arguments(List.of(99L), Map.of()), null, Map.of())));
        assertThrows(IllegalArgumentException.class, () -> fn.onBind(new ScalarBindParams(
                "format_number", new Arguments(List.of(-1L), Map.of()), null, Map.of())));
    }

    @Test
    void sharedEnforcerRejectsOutOfRangeConst() {
        // The shared ConstraintEnforcer is what every bind path now calls
        // (scalar/table/table-in-out/buffering/aggregate) — verify it directly.
        List<ArgSpec> specs = new FormatNumber().argumentSpecs();
        farm.query.vgi.function.ConstraintEnforcer.enforce(new Arguments(List.of(2L), Map.of()), specs);
        assertThrows(IllegalArgumentException.class, () ->
                farm.query.vgi.function.ConstraintEnforcer.enforce(
                        new Arguments(List.of(99L), Map.of()), specs));
    }
}
