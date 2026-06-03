// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Setting;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.BigIntVector;

/** {@code multiply_by_setting(value: int64) -> int64}: multiplies by the {@code multiplier} session setting. */
public final class MultiplyBySettingFunction extends ScalarFn {

    @Override public String name() { return "multiply_by_setting"; }
    @Override public String description() { return "Multiply the input value by a setting value"; }

    public void compute(
            @Vector BigIntVector value,
            @Setting(default_ = "1") long multiplier,
            BigIntVector result) {
        int rows = value.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (value.isNull(i)) result.setNull(i);
            else result.setSafe(i, value.get(i) * multiplier);
        }
    }
}
