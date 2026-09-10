// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.scalar.Const;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFn;
import farm.query.vgi.scalar.Vector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/** Verifies that bind receives the complete resolved VGI 2 function signature. */
public final class ArgumentNamesProbeFunction extends ScalarFn {
    private final VectorSchemaRoot defaults;

    public ArgumentNamesProbeFunction() {
        defaults = VectorSchemaRoot.create(new Schema(List.of(
                new Field("scale", FieldType.nullable(new ArrowType.Int(64, true)), null))),
                Allocators.root());
        defaults.allocateNew();
        ((BigIntVector) defaults.getVector("scale")).setSafe(0, 2L);
        defaults.getVector("scale").setValueCount(1);
        defaults.setRowCount(1);
    }

    @Override public String name() { return "argument_names_probe"; }
    @Override public String description() { return "Checks VGI 2.0 bind-time argument names"; }
    @Override public VectorSchemaRoot parameterDefaultValues() { return defaults; }

    @Override
    protected void validateBind(ScalarBindParams params) {
        if (params.inputSchema() == null) {
            return;
        }
        List<String> expected = List.of("left", "right", "scale");
        if (!expected.equals(params.argumentNames())) {
            throw new IllegalArgumentException(
                    "argument_names_probe expected " + expected + ", got " + params.argumentNames());
        }
    }

    public void compute(
            @Vector BigIntVector left,
            @Vector BigIntVector right,
            @Const long scale,
            BigIntVector result) {
        for (int i = 0; i < left.getValueCount(); i++) {
            if (left.isNull(i) || right.isNull(i)) {
                result.setNull(i);
            } else {
                result.setSafe(i, Math.multiplyExact(Math.addExact(left.get(i), right.get(i)), scale));
            }
        }
    }
}
