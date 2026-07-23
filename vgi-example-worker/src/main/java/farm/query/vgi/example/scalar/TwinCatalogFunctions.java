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
 * {@code test_same_name_catalog(value BIGINT) -> VARCHAR} — two catalogs, one
 * worker process, one function name.
 *
 * <p>{@code twin_a} and {@code twin_b} are separate VGI catalogs served by the
 * SAME worker process. Each declares a schema literally named {@code main}
 * holding a scalar literally named {@code test_same_name_catalog}, so neither
 * the function name nor the schema name distinguishes them — only the attached
 * catalog does. Routing therefore has to key on the per-attach
 * {@code attach_opaque_data}, which is what names the catalog; the bind
 * request's {@code schema_name} cannot help when both schemas are {@code main}.
 *
 * <p>Companion to {@link SameNameFunctions}, which collides one name across two
 * schemas of a SINGLE catalog. Mirrors vgi-python's
 * {@code vgi/_test_fixtures/twin_catalogs.py}; driven by
 * {@code test/sql/integration/scalar/same_name_catalogs.test}.
 */
public final class TwinCatalogFunctions {

    private TwinCatalogFunctions() {}

    /** The colliding registered name — deliberately identical in both catalogs. */
    public static final String NAME = "test_same_name_catalog";

    /** The schema both catalogs declare it in — also deliberately identical. */
    public static final String SCHEMA = "main";

    /** First twin catalog. */
    public static final String CATALOG_A = "twin_a";

    /** Second twin catalog. */
    public static final String CATALOG_B = "twin_b";

    /** Render {@code <catalog>:<value>} for every row, preserving nulls. */
    private static void tag(String catalog, BigIntVector value, VarCharVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            result.setSafe(i, new Text(catalog + ":" + value.get(i)));
        }
    }

    /** {@code test_same_name_catalog} as served by the {@code twin_a} catalog. */
    public static final class TwinA extends ScalarFn {

        @Override public String name() { return NAME; }

        @Override public String description() {
            return "Catalog-disambiguation probe; the twin_a implementation";
        }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description())
                    .withCategories("test")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT a.main.test_same_name_catalog(1)", "Returns 'twin_a:1'", null)));
        }

        /**
         * Tag each value with the owning catalog.
         *
         * @param value  the integer values to tag
         * @param result the {@code twin_a:<value>} strings
         */
        public void compute(@Vector BigIntVector value, VarCharVector result) {
            tag(CATALOG_A, value, result);
        }
    }

    /** {@code test_same_name_catalog} as served by the {@code twin_b} catalog. */
    public static final class TwinB extends ScalarFn {

        @Override public String name() { return NAME; }

        @Override public String description() {
            return "Catalog-disambiguation probe; the twin_b implementation";
        }

        @Override public FunctionMetadata metadata() {
            return FunctionMetadata.describe(description())
                    .withCategories("test")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT b.main.test_same_name_catalog(1)", "Returns 'twin_b:1'", null)));
        }

        /**
         * Tag each value with the owning catalog.
         *
         * @param value  the integer values to tag
         * @param result the {@code twin_b:<value>} strings
         */
        public void compute(@Vector BigIntVector value, VarCharVector result) {
            tag(CATALOG_B, value, result);
        }
    }
}
