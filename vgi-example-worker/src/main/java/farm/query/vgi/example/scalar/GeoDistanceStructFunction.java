// Copyright 2025-2026 Query.Farm LLC

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
import org.apache.arrow.vector.complex.StructVector;

/** {@code geo_distance_struct(p1 STRUCT(lat,lon), p2 STRUCT(lat,lon)) -> DOUBLE}. */
public final class GeoDistanceStructFunction implements ScalarFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("geo_distance_struct")
            .description("Euclidean distance between two struct points")
            .nested("p1", GeoTypes.structArgType(), GeoTypes.structPointChildren())
            .nested("p2", GeoTypes.structArgType(), GeoTypes.structPointChildren())
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(ScalarBindParams params) {
        return BindResponse.forSchema(Schemas.singleResultIpc(Schemas.FLOAT64));
    }

    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        StructVector p1 = (StructVector) input.getFieldVectors().get(0);
        StructVector p2 = (StructVector) input.getFieldVectors().get(1);
        Float8Vector p1lat = (Float8Vector) p1.getChildByOrdinal(0);
        Float8Vector p1lon = (Float8Vector) p1.getChildByOrdinal(1);
        Float8Vector p2lat = (Float8Vector) p2.getChildByOrdinal(0);
        Float8Vector p2lon = (Float8Vector) p2.getChildByOrdinal(1);

        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        Float8Vector v = (Float8Vector) out.getVector("result");
        for (int i = 0; i < rows; i++) {
            if (p1.isNull(i) || p2.isNull(i)) {
                v.setNull(i);
            } else {
                v.setSafe(i, GeoTypes.euclidean(
                        p1lat.get(i), p1lon.get(i), p2lat.get(i), p2lon.get(i)));
            }
        }
        out.setRowCount(rows);
        return out;
    }
}
