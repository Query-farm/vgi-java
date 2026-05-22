// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.List;

/** {@code geo_distance_struct(p1 STRUCT(lat,lon), p2 STRUCT(lat,lon)) -> DOUBLE}. */
public final class GeoDistanceStructFunction extends ScalarFn {

    @Override public String name() { return "geo_distance_struct"; }
    @Override public String description() { return "Euclidean distance between two struct points"; }

    @Override
    public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.nested("p1", 0, GeoTypes.structArgType(), GeoTypes.structPointChildren(), false),
                ArgSpec.nested("p2", 1, GeoTypes.structArgType(), GeoTypes.structPointChildren(), false));
    }

    public void compute(
            @Vector StructVector p1,
            @Vector StructVector p2,
            Float8Vector result) {
        Float8Vector p1lat = (Float8Vector) p1.getChildByOrdinal(0);
        Float8Vector p1lon = (Float8Vector) p1.getChildByOrdinal(1);
        Float8Vector p2lat = (Float8Vector) p2.getChildByOrdinal(0);
        Float8Vector p2lon = (Float8Vector) p2.getChildByOrdinal(1);
        int rows = p1.getValueCount();
        for (int i = 0; i < rows; i++) {
            if (p1.isNull(i) || p2.isNull(i)) { result.setNull(i); continue; }
            result.setSafe(i, GeoTypes.euclidean(
                    p1lat.get(i), p1lon.get(i), p2lat.get(i), p2lon.get(i)));
        }
    }
}
