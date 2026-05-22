// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;

/** {@code upper_case(value: utf8) -> utf8}. */
public final class UpperCaseFunction extends ScalarFn {

    @Override public String name() { return "upper_case"; }
    @Override public String description() { return "Converts string values to uppercase"; }

    public void compute(@Vector VarCharVector value, VarCharVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) result.setNull(i);
            else result.setSafe(i, new Text(value.getObject(i).toString().toUpperCase()));
        }
    }
}
