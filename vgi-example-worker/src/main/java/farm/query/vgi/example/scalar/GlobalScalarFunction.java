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
 * {@code global_scalar(value: int64) -> utf8} — the scalar member of the
 * global-registration probe family (the others are
 * {@code table/GlobalTableFunction}, {@code aggregate/GlobalAggFunction} and
 * {@code buffering/GlobalBufferedFunction}).
 *
 * <p>One probe per function kind, so a client publishing a worker's functions
 * into its <em>global</em> namespace exercises every registration path. They are
 * deliberately new rather than reused fixtures: the example catalog is a
 * cross-language contract, and reusing {@code double} / {@code ten_thousand} /
 * {@code vgi_sum} / {@code echo_buffering} would force the same semantic change
 * on every SDK's existing functions.
 *
 * <p>Each returns a value tagged with its own name, so a test can prove the
 * globally published name reached the function it was supposed to rather than a
 * same-named function belonging to another catalog. Mirrors vgi-python's
 * {@code _test_fixtures/global_functions.py}.
 */
public final class GlobalScalarFunction extends ScalarFn {

    @Override public String name() { return "global_scalar"; }

    @Override public String description() { return "Global-registration probe (scalar)"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe(description())
                .withCategories("test", "global")
                .withExamples(List.of(new FunctionExample(
                        "SELECT vgi_example_global_scalar(7)",
                        "Scalar probe published into system.main", null)));
    }

    /**
     * Labels each row {@code global_scalar:<value>}; NULL in, NULL out.
     *
     * @param value the values to label
     * @param result framework-allocated output column
     */
    public void compute(@Vector BigIntVector value, VarCharVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) {
                result.setNull(i);
                continue;
            }
            result.setSafe(i, new Text("global_scalar:" + value.get(i)));
        }
    }
}
