// VGI-Java example: a scalar function.
//
// A scalar function maps each input row to one output row. You extend
// `ScalarFn` and write a single `compute()` method; the framework reads its
// parameter annotations to derive the SQL signature, the output type, and the
// per-batch dispatch. There is no schema boilerplate to write by hand.
//
// Run it on its own:
//   ./gradlew runScalar --args="--unix /tmp/scalar.sock --idle-timeout 60"
// then from Haybarn:
//   ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:/abs/path/bin/runScalar');
//   SELECT demo.upper_case('hello');   -- HELLO
package farm.query.vgi.examples;

import farm.query.vgi.Worker;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.VarCharVector;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/** {@code upper_case(value VARCHAR) -> VARCHAR}: ASCII/Unicode uppercase. */
public final class ScalarExample extends ScalarFn {

    @Override public String name() { return "upper_case"; }
    @Override public String description() { return "Uppercase a string"; }

    // One `@Vector` input column + one trailing (unannotated) output vector.
    // The framework allocates `result`, sized to the batch row count, and
    // writes whatever you put into it back across the wire.
    //
    // Parameter rules in one breath:
    //   @Vector  -> a per-row input column (the Arrow vector type is the SQL type)
    //   @Const   -> a bind-time constant arg (long/double/String/boolean/byte[])
    //   @Setting -> a session setting (SET demo.foo = ...)
    //   last unannotated vector = the output (framework-allocated)
    public void compute(@Vector VarCharVector value, VarCharVector result) {
        int rows = value.getValueCount();
        result.allocateNew();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) { result.setNull(i); continue; }
            String up = new String(value.get(i), StandardCharsets.UTF_8).toUpperCase(Locale.ROOT);
            byte[] bytes = up.getBytes(StandardCharsets.UTF_8);
            result.setSafe(i, bytes, 0, bytes.length);
        }
    }

    public static void main(String[] args) {
        Worker.builder()
                .catalogName("demo")
                .registerScalar(new ScalarExample())
                .runFromArgs(args);   // handles --unix / --http / --idle-timeout / stdio
    }
}
