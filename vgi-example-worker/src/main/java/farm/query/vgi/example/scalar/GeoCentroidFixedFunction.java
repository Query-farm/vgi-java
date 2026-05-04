// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;
import org.apache.arrow.vector.complex.StructVector;

import java.util.ArrayList;
import java.util.List;

/** {@code geo_centroid_fixed(points DOUBLE[2]...) -> STRUCT(lat,lon)} (varargs). */
public final class GeoCentroidFixedFunction implements ScalarFunction {

    @Override public String name() { return "geo_centroid_fixed"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Centroid of N fixed-size list points");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.nested(
                "points", 0, GeoTypes.fixedListArgType(), GeoTypes.doubleElementChildren(), /*varargs=*/true));
    }
    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(GeoTypes.CENTROID_OUTPUT_SCHEMA_IPC);
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        int numCols = input.getFieldVectors().size();
        int rows = input.getRowCount();

        List<FixedSizeListVector> fsls = new ArrayList<>(numCols);
        List<Float8Vector> values = new ArrayList<>(numCols);
        for (FieldVector fv : input.getFieldVectors()) {
            FixedSizeListVector lv = (FixedSizeListVector) fv;
            fsls.add(lv);
            values.add((Float8Vector) lv.getDataVector());
        }

        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        StructVector result = (StructVector) out.getVector("result");
        Float8Vector outLat = (Float8Vector) result.getChildByOrdinal(0);
        Float8Vector outLon = (Float8Vector) result.getChildByOrdinal(1);

        for (int i = 0; i < rows; i++) {
            boolean anyNull = false;
            for (FixedSizeListVector fsl : fsls) {
                if (fsl.isNull(i)) { anyNull = true; break; }
            }
            if (anyNull) {
                result.setNull(i);
                continue;
            }
            double sumLat = 0, sumLon = 0;
            for (int c = 0; c < numCols; c++) {
                sumLat += values.get(c).get(i * 2);
                sumLon += values.get(c).get(i * 2 + 1);
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
