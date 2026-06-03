// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.internal;

import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.types.pojo.Field;

/**
 * Builds a one-row {@link FieldVector} carrying an ATTACH option's default
 * value. Called <em>once</em> per spec at worker registration; on the merge
 * hot path the resulting vector is copied via {@code TransferPair} so this
 * is the only place Java→Arrow type dispatch lives.
 */
public final class AttachOptionDefaultMaterializer {

    private AttachOptionDefaultMaterializer() {}

    /**
     * Allocate a length-1 vector for {@code field} and write {@code value}
     * into row 0. Caller owns the returned vector.
     *
     * @param field the field describing the vector type
     * @param value the Java value to write into row 0
     * @return a freshly-allocated one-row vector
     */
    public static FieldVector materialize(Field field, Object value) {
        FieldVector v = field.createVector(Allocators.root());
        v.allocateNew();
        VectorScalarCodec.write(v, 0, value);
        v.setValueCount(1);
        return v;
    }
}
