// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.FunctionExample;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

import java.util.List;

/**
 * {@code test_same_name_bind(value BIGINT) -> VARCHAR} — the SAME function name
 * registered in two different schemas of the {@code example} catalog.
 *
 * <p>Two distinct implementations share one registered name but live in
 * different schemas ({@code main} and {@code data}). They exist to prove that
 * VGI resolves a schema-qualified call to the implementation in the schema the
 * caller named — {@code example.main.test_same_name_bind(x)} must reach the
 * main class and {@code example.data.test_same_name_bind(x)} the data class —
 * rather than collapsing both into one flat by-name registry entry, where they
 * would collide as indistinguishable overloads.
 *
 * <p>Each tags its output with its own schema, so a mis-routed call shows up in
 * the query result rather than as a plausible answer. Mirrors vgi-python's
 * {@code vgi/_test_fixtures/scalar/same_name.py}; driven by
 * {@code test/sql/integration/scalar/same_name_schemas.test}.
 */
public final class SameNameFunctions {

    private SameNameFunctions() {}

    /** The colliding registered name — deliberately identical in both schemas. */
    public static final String NAME = "test_same_name_bind";

    /** Render {@code <tag>:<value>} for every row, preserving nulls. */
    private static void tag(String label, BigIntVector value, VarCharVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            result.setSafe(i, new Text(label + ":" + value.get(i)));
        }
    }

    /** {@code test_same_name_bind} as registered in the {@code main} schema. */
    public static final class MainSchema extends ScalarFn {

        @Override public String name() { return NAME; }

        @Override public String description() {
            return "Schema-disambiguation probe; the main-schema implementation";
        }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description())
                    .withCategories("test")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT example.main.test_same_name_bind(1)", "Returns 'main:1'", null)));
        }

        /**
         * Tag each value with the owning schema.
         *
         * @param value  the integer values to tag
         * @param result the {@code main:<value>} strings
         */
        public void compute(@Vector BigIntVector value, VarCharVector result) {
            tag("main", value, result);
        }
    }

    /** {@code test_same_name_bind} as registered in the {@code data} schema. */
    public static final class DataSchema extends ScalarFn {

        @Override public String name() { return NAME; }

        @Override public String description() {
            return "Schema-disambiguation probe; the data-schema implementation";
        }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description())
                    .withCategories("test")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT example.data.test_same_name_bind(1)", "Returns 'data:1'", null)));
        }

        /**
         * Tag each value with the owning schema.
         *
         * @param value  the integer values to tag
         * @param result the {@code data:<value>} strings
         */
        public void compute(@Vector BigIntVector value, VarCharVector result) {
            tag("data", value, result);
        }
    }
}
