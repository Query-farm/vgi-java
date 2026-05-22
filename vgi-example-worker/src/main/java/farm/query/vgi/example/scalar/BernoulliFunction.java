// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.NullHandling;
import farm.query.vgi.function.Stability;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.concurrent.ThreadLocalRandom;

/** {@code bernoulli() -> BOOLEAN}, VOLATILE — emits a random bit per row. */
public final class BernoulliFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.BOOL);
    private static final FunctionMetadata META = new FunctionMetadata(
            "Generate random booleans (demonstrates VOLATILE stability)",
            Stability.VOLATILE, NullHandling.DEFAULT, false, false, false, false);

    private static final FunctionSpec SPEC = FunctionSpec.builder("bernoulli")
            .metadata(META)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(ScalarBindParams p) { return BindResponse.forSchema(OUTPUT_SCHEMA_IPC); }
    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        BitVector v = (BitVector) out.getVector("result");
        for (int i = 0; i < rows; i++) v.setSafe(i, ThreadLocalRandom.current().nextBoolean() ? 1 : 0);
        out.setRowCount(rows);
        return out;
    }
}
