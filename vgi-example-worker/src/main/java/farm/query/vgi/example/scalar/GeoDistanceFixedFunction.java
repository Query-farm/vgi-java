// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.FixedSizeListVector;

import java.util.List;

/** {@code geo_distance_fixed(p1 DOUBLE[2], p2 DOUBLE[2]) -> DOUBLE}. */
public final class GeoDistanceFixedFunction extends ScalarFn {

    @Override public String name() { return "geo_distance_fixed"; }
    @Override public String description() { return "Euclidean distance between two fixed-size list points"; }

    @Override
    public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.nested("p1", 0, GeoTypes.fixedListArgType(), GeoTypes.doubleElementChildren(), false),
                ArgSpec.nested("p2", 1, GeoTypes.fixedListArgType(), GeoTypes.doubleElementChildren(), false));
    }

    public void compute(
            @Vector FixedSizeListVector p1,
            @Vector FixedSizeListVector p2,
            Float8Vector result) {
        Float8Vector p1vals = (Float8Vector) p1.getDataVector();
        Float8Vector p2vals = (Float8Vector) p2.getDataVector();
        int rows = p1.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (p1.isNull(i) || p2.isNull(i)) { result.setNull(i); continue; }
            double lat1 = p1vals.get(i * 2);
            double lon1 = p1vals.get(i * 2 + 1);
            double lat2 = p2vals.get(i * 2);
            double lon2 = p2vals.get(i * 2 + 1);
            result.setSafe(i, GeoTypes.euclidean(lat1, lon1, lat2, lon2));
        }
    }
}
