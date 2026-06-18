// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.scalar;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.scalar.ScalarBindParams;
import farm.query.vgi.scalar.ScalarFunction;
import farm.query.vgi.scalar.ScalarProcessParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.util.List;

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
        String port = "";
        String name = "";
        if (bytes != null && bytes.length > 0) {
            try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
                 ArrowStreamReader r = new ArrowStreamReader(in, Allocators.root())) {
                if (r.loadNextBatch()) {
                    VectorSchemaRoot root = r.getVectorSchemaRoot();
                    for (Field f : root.getSchema().getFields()) {
                        FieldVector vv = root.getVector(f.getName());
                        if (vv instanceof StructVector sv) {
                            FieldVector pc = sv.getChild("port");
                            if (pc != null && !pc.isNull(0)) port = String.valueOf(pc.getObject(0));
                            FieldVector nc = sv.getChild("secret_string");
                            if (nc != null && !nc.isNull(0)) name = String.valueOf(nc.getObject(0));
                        }
                    }
                }
            } catch (Exception ignore) {
                // Resolve failure → empty fields, matching the python fixture.
            }
        }
        return "port=" + port + ";name=" + name;
    }
}
