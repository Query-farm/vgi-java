// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;

/** {@code geo_distance_list(p1 LIST<DOUBLE>, p2 LIST<DOUBLE>) -> DOUBLE}. */
public final class GeoDistanceListFunction implements ScalarFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("geo_distance_list")
            .description("Euclidean distance between two list points")
            .nested("p1", GeoTypes.listArgType(), GeoTypes.doubleElementChildren())
            .nested("p2", GeoTypes.listArgType(), GeoTypes.doubleElementChildren())
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(Schemas.singleResultIpc(Schemas.FLOAT64));
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        ListVector p1 = (ListVector) input.getFieldVectors().get(0);
        ListVector p2 = (ListVector) input.getFieldVectors().get(1);
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
            int s1 = p1.getElementStartIndex(i);
            int s2 = p2.getElementStartIndex(i);
            double lat1 = p1vals.get(s1);
            double lon1 = p1vals.get(s1 + 1);
            double lat2 = p2vals.get(s2);
            double lon2 = p2vals.get(s2 + 1);
            v.setSafe(i, GeoTypes.euclidean(lat1, lon1, lat2, lon2));
        }
        out.setRowCount(rows);
        return out;
    }
}
