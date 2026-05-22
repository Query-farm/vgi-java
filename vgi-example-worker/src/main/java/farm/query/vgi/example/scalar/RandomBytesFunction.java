// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Random;

/** {@code random_bytes(seed BIGINT [const], byte_length BIGINT [const]) -> BLOB} — deterministic. */
public final class RandomBytesFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.BINARY);

    private static final FunctionSpec SPEC = FunctionSpec.builder("random_bytes")
            .description("Generate pseudo-random binary blobs from seed and length")
            .constArg("seed", Schemas.INT64)
            .constArg("byte_length", Schemas.INT64)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
    @Override public BindResponse onBind(ScalarBindParams p) {
        Object lenObj = p.arguments().named().get("byte_length");
        if (lenObj == null) lenObj = p.arguments().named().get("named_byte_length");
        if (lenObj == null && p.arguments().positional().size() > 1) {
            lenObj = p.arguments().positional().get(1);
        }
        if (lenObj instanceof Number n && n.longValue() < 0) {
            throw new IllegalArgumentException("byte_length must be >= 0");
        }
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }
    @Override
    public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input, BufferAllocator alloc) {
        ParameterExtractor p = ParameterExtractor.of(params.arguments());
        long seed = p.positional(0, "seed").asLong().required();
        long byteLen = p.positional(1, "byte_length").asLong().ge(0).required();
        Random rng = new Random(seed);
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        VarBinaryVector v = (VarBinaryVector) out.getVector("result");
        for (int i = 0; i < rows; i++) {
            byte[] buf = new byte[(int) byteLen];
            rng.nextBytes(buf);
            v.setSafe(i, buf);
        }
        out.setRowCount(rows);
        return out;
    }
}
