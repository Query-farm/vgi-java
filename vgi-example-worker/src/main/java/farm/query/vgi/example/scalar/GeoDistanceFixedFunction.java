// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.FixedSizeListVector;

import java.util.List;

/** {@code geo_distance_fixed(p1 DOUBLE[2], p2 DOUBLE[2]) -> DOUBLE}. */
public final class GeoDistanceFixedFunction implements ScalarFunction {

    @Override public String name() { return "geo_distance_fixed"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Euclidean distance between two fixed-size list points");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.nested("p1", 0, GeoTypes.fixedListArgType(), GeoTypes.doubleElementChildren(), false),
                ArgSpec.nested("p2", 1, GeoTypes.fixedListArgType(), GeoTypes.doubleElementChildren(), false));
    }
    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(Schemas.singleResultIpc(Schemas.FLOAT64));
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        FixedSizeListVector p1 = (FixedSizeListVector) input.getFieldVectors().get(0);
        FixedSizeListVector p2 = (FixedSizeListVector) input.getFieldVectors().get(1);
        Float8Vector p1vals = (Float8Vector) p1.getDataVector();
        Float8Vector p2vals = (Float8Vector) p2.getDataVector();

        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        Float8Vector v = (Float8Vector) out.getVector("result");
        for (int i = 0; i < rows; i++) {
            if (p1.isNull(i) || p2.isNull(i)) {
                v.setNull(i);
                continue;
            }
            double lat1 = p1vals.get(i * 2);
            double lon1 = p1vals.get(i * 2 + 1);
            double lat2 = p2vals.get(i * 2);
            double lon2 = p2vals.get(i * 2 + 1);
            v.setSafe(i, GeoTypes.euclidean(lat1, lon1, lat2, lon2));
        }
        out.setRowCount(rows);
        return out;
    }
}
