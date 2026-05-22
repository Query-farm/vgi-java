// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

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
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.util.List;
import java.util.TreeMap;

/**
 * {@code return_secret_value()} — returns a JSON-shaped VARCHAR encoding of
 * the resolved {@code vgi_example} secret's user-supplied fields, or NULL if
 * no secret is in scope.
 */
public final class ReturnSecretValueFunction implements ScalarFunction {

    private static final byte[] OUTPUT_SCHEMA_IPC = Schemas.singleResultIpc(Schemas.UTF8);
    private static final java.util.Set<String> PROTOCOL_FIELDS =
            java.util.Set.of("name", "type", "provider");

    private static final FunctionSpec SPEC = FunctionSpec.builder("return_secret_value")
            .description("Return a secret's value")
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
        String json = encodeSecretJson(params.secrets());
        Text t = json == null ? null : new Text(json);
        for (int i = 0; i < rows; i++) {
            if (t == null) v.setNull(i);
            else v.setSafe(i, t);
        }
        out.setRowCount(rows);
        return out;
    }

    private static String encodeSecretJson(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return null;
        TreeMap<String, Object> kv = new TreeMap<>();
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
             ArrowStreamReader r = new ArrowStreamReader(in, Allocators.root())) {
            if (r.loadNextBatch()) {
                VectorSchemaRoot root = r.getVectorSchemaRoot();
                for (Field f : root.getSchema().getFields()) {
                    FieldVector vv = root.getVector(f.getName());
                    if (vv == null || vv.isNull(0)) continue;
                    if (vv instanceof org.apache.arrow.vector.complex.StructVector sv) {
                        for (Field child : f.getChildren()) {
                            String name = child.getName();
                            if (PROTOCOL_FIELDS.contains(name)) continue;
                            FieldVector cv = sv.getChild(name);
                            if (cv == null || cv.isNull(0)) continue;
                            kv.put(name, cv.getObject(0));
                        }
                    }
                }
            }
        } catch (Exception ignore) {
            return null;
        }
        if (kv.isEmpty()) return null;
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var e : kv.entrySet()) {
            if (!first) sb.append(", ");
            first = false;
            sb.append('"').append(e.getKey()).append("\": ");
            Object val = e.getValue();
            if (val instanceof CharSequence || val instanceof org.apache.arrow.vector.util.Text) {
                sb.append('"').append(String.valueOf(val).replace("\"", "\\\"")).append('"');
            } else {
                sb.append(String.valueOf(val));
            }
        }
        sb.append("}");
        return sb.toString();
    }
}
