// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.ListVector;

import java.util.List;

/** {@code geo_distance_list(p1 LIST<DOUBLE>, p2 LIST<DOUBLE>) -> DOUBLE}. */
public final class GeoDistanceListFunction extends ScalarFn {

    @Override public String name() { return "geo_distance_list"; }
    @Override public String description() { return "Euclidean distance between two list points"; }

    @Override
    public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.nested("p1", 0, GeoTypes.listArgType(), GeoTypes.doubleElementChildren(), false),
                ArgSpec.nested("p2", 1, GeoTypes.listArgType(), GeoTypes.doubleElementChildren(), false));
    }

    public void compute(
            @Vector ListVector p1,
            @Vector ListVector p2,
            Float8Vector result) {
        Float8Vector p1vals = (Float8Vector) p1.getDataVector();
        Float8Vector p2vals = (Float8Vector) p2.getDataVector();
        int rows = p1.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (p1.isNull(i) || p2.isNull(i)) { result.setNull(i); continue; }
            int s1 = p1.getElementStartIndex(i);
            int s2 = p2.getElementStartIndex(i);
            double lat1 = p1vals.get(s1), lon1 = p1vals.get(s1 + 1);
            double lat2 = p2vals.get(s2), lon2 = p2vals.get(s2 + 1);
            result.setSafe(i, GeoTypes.euclidean(lat1, lon1, lat2, lon2));
        }
    }
}
