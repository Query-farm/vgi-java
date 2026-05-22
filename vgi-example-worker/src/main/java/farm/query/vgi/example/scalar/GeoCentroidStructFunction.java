// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** {@code geo_centroid_struct(points STRUCT(lat,lon)...) -> STRUCT(lat,lon)} — varargs. */
public final class GeoCentroidStructFunction extends ScalarFn {

    @Override public String name() { return "geo_centroid_struct"; }
    @Override public String description() { return "Centroid of N struct points"; }

    @Override
    public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.nested("points", 0,
                GeoTypes.structArgType(), GeoTypes.structPointChildren(), /*varargs=*/true));
    }

    @Override
    protected Schema outputSchema(Schema inputSchema, Arguments arguments) {
        return GeoTypes.CENTROID_OUTPUT_SCHEMA;
    }

    public void compute(
            @Vector(varargs = true) List<StructVector> points,
            StructVector result) {
        Float8Vector outLat = (Float8Vector) result.getChildByOrdinal(0);
        Float8Vector outLon = (Float8Vector) result.getChildByOrdinal(1);
        int rows = points.get(0).getValueCount();
        int numCols = points.size();
        for (int i = 0; i < rows; i++) {
            boolean anyNull = false;
            for (StructVector s : points) {
                if (s.isNull(i)) { anyNull = true; break; }
            }
            if (anyNull) { result.setNull(i); continue; }
            double sumLat = 0, sumLon = 0;
            for (StructVector s : points) {
                sumLat += ((Float8Vector) s.getChildByOrdinal(0)).get(i);
                sumLon += ((Float8Vector) s.getChildByOrdinal(1)).get(i);
            }
            outLat.setSafe(i, sumLat / numCols);
            outLon.setSafe(i, sumLon / numCols);
            result.setIndexDefined(i);
        }
    }
}
