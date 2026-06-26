// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.Secrets;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.util.List;
import java.util.Map;

/**
 * {@code secret_field()} — looks up individual fields on the resolved
 * {@code vgi_example} secret and renders {@code port=<port>;name=<secret_string>}.
 * Mirrors vgi-python's {@code SecretFieldFunction} (named/by-field accessors).
 */
public final class SecretFieldFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);

    private static final FunctionSpec SPEC = FunctionSpec.builder("secret_field")
            .description("Look up secret fields by name")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(ScalarBindParams p) {
        if (!p.resolvedSecretsProvided()) {
            return new BindResponse(OUTPUT_SCHEMA_IPC, new byte[0],
                    List.of("vgi_example"), List.of(""), List.of(""));
        }
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public VectorSchemaRoot process(ScalarProcessParams params, VectorSchemaRoot input,
                                                 BufferAllocator alloc) {
        int rows = input.getRowCount();
        VectorSchemaRoot out = VectorSchemaRoot.create(params.outputSchema(), alloc);
        out.allocateNew();
        VarCharVector v = (VarCharVector) out.getVector("result");
        Text t = new Text(formatSecretField(params.secrets()));
        for (int i = 0; i < rows; i++) v.setSafe(i, t);
        out.setRowCount(rows);
        return out;
    }

    private static String formatSecretField(byte[] bytes) {
        // Secrets are keyed by name; select the vgi_example-typed secret.
        Map<String, String> s = Secrets.parse(bytes).ofType("vgi_example")
                .stream().findFirst().orElse(Map.of());
        return "port=" + s.getOrDefault("port", "") + ";name=" + s.getOrDefault("secret_string", "");
    }
}
