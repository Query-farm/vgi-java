// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Setting;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.Float8Vector;

/**
 * {@code scale_by_setting(value: double) -> double}: multiplies by the
 * {@code scale_factor} DOUBLE session setting (read via {@code get_f64}).
 * Float counterpart to {@link MultiplyBySettingFunction}.
 */
public final class ScaleBySettingFunction extends ScalarFn {

    @Override public String name() { return "scale_by_setting"; }
    @Override public String description() {
        return "Scale the input value by the float setting `scale_factor`";
    }

    public void compute(
            @Vector Float8Vector value,
            @Setting(default_ = "1.0") double scale_factor,
            Float8Vector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) result.setNull(i);
            else result.setSafe(i, value.get(i) * scale_factor);
        }
    }
}
