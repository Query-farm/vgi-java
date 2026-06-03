// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/** {@code whoami(x: int64) -> utf8}: always returns {@code "anonymous"} (auth wiring is Phase 8). */
public final class WhoAmIFunction extends ScalarFn {

    @Override public String name() { return "whoami"; }
    @Override public String description() { return "Return the authenticated principal name."; }

    public void compute(@Vector BigIntVector x, VarCharVector result) {
        Text anon = new Text("anonymous");
        int rows = x.getValueCount();
        for (int i = 0; i < rows; i++) result.setSafe(i, anon);
    }
}
