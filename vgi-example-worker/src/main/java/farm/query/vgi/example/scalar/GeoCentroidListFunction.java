// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** {@code geo_centroid_list(points LIST<DOUBLE>...) -> STRUCT(lat,lon)} — varargs. */
public final class GeoCentroidListFunction extends ScalarFn {

    @Override public String name() { return "geo_centroid_list"; }
    @Override public String description() { return "Centroid of N list points"; }

    @Override
    public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.nested("points", 0,
                GeoTypes.listArgType(), GeoTypes.doubleElementChildren(), /*varargs=*/true));
    }

    @Override
    protected Schema outputSchema(Schema inputSchema, Arguments arguments) {
        return GeoTypes.CENTROID_OUTPUT_SCHEMA;
    }

    public void compute(
            @Vector(varargs = true) List<ListVector> points,
            StructVector result) {
        Float8Vector outLat = (Float8Vector) result.getChildByOrdinal(0);
        Float8Vector outLon = (Float8Vector) result.getChildByOrdinal(1);
        int rows = points.get(0).getValueCount();
        int numCols = points.size();
        for (int i = 0; i < rows; i++) {
            boolean anyNull = false;
            for (ListVector lv : points) {
                if (lv.isNull(i)) { anyNull = true; break; }
            }
            if (anyNull) { result.setNull(i); continue; }
            double sumLat = 0, sumLon = 0;
            for (ListVector lv : points) {
                int s = lv.getElementStartIndex(i);
                Float8Vector vals = (Float8Vector) lv.getDataVector();
                sumLat += vals.get(s);
                sumLon += vals.get(s + 1);
            }
            outLat.setSafe(i, sumLat / numCols);
            outLon.setSafe(i, sumLon / numCols);
            result.setIndexDefined(i);
        }
    }
}
