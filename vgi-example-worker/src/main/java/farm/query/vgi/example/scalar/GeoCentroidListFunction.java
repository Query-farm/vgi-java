// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.ArrayList;
import java.util.List;

/** {@code geo_centroid_list(points LIST<DOUBLE>...) -> STRUCT(lat,lon)} (varargs). */
public final class GeoCentroidListFunction implements ScalarFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("geo_centroid_list")
            .description("Centroid of N list points")
            .arg(ArgSpec.nested(
                    "points", 0, GeoTypes.listArgType(), GeoTypes.doubleElementChildren(), /*varargs=*/true))
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(GeoTypes.CENTROID_OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        int numCols = input.getFieldVectors().size();
        int rows = input.getRowCount();

        List<ListVector> lists = new ArrayList<>(numCols);
        List<Float8Vector> values = new ArrayList<>(numCols);
        for (FieldVector fv : input.getFieldVectors()) {
            ListVector lv = (ListVector) fv;
            lists.add(lv);
            values.add((Float8Vector) lv.getDataVector());
        }

        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        StructVector result = (StructVector) out.getVector("result");
        Float8Vector outLat = (Float8Vector) result.getChildByOrdinal(0);
        Float8Vector outLon = (Float8Vector) result.getChildByOrdinal(1);

        for (int i = 0; i < rows; i++) {
            boolean anyNull = false;
            for (ListVector lv : lists) {
                if (lv.isNull(i)) { anyNull = true; break; }
            }
            if (anyNull) {
                result.setNull(i);
                continue;
            }
            double sumLat = 0, sumLon = 0;
            for (int c = 0; c < numCols; c++) {
                int s = lists.get(c).getElementStartIndex(i);
                sumLat += values.get(c).get(s);
                sumLon += values.get(c).get(s + 1);
            }
            outLat.setSafe(i, sumLat / numCols);
            outLon.setSafe(i, sumLon / numCols);
            result.setIndexDefined(i);
        }
        result.setValueCount(rows);
        out.setRowCount(rows);
        return out;
    }
}
