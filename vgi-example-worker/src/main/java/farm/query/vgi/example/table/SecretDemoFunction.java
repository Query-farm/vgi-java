// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.ByteArrayInputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * {@code secret_demo()} — outputs the resolved {@code vgi_example} secret as
 * (key, value, arrow_type) rows. Returns no rows when no secret is in scope.
 */
public final class SecretDemoFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("key", Schemas.UTF8),
            Schemas.nullable("value", Schemas.UTF8),
            Schemas.nullable("arrow_type", Schemas.UTF8)));

    /** IPC-serialised output schema {@code (key, value, arrow_type)}, all Utf8.
     *  Reused by the {@code secret_demo_table} catalog registration. */
    public static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    private static final FunctionSpec SPEC = FunctionSpec.builder("secret_demo")
            .description("Outputs secret contents as key-value rows")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableBindParams p) {
        // Two-phase bind: if DuckDB hasn't supplied the resolved secret, ask
        // for one of type vgi_example. DuckDB re-issues bind with
        // resolved_secrets_provided=true and the resolved bytes (or skips if
        // no matching secret is in scope).
        if (!p.resolvedSecretsProvided()) {
            return new BindResponse(OUTPUT_SCHEMA_IPC, new byte[0],
                    List.of("vgi_example"), List.of(""), List.of(""));
        }
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        return new State(decodeSecret(params.secrets()));
    }

    private static List<String[]> decodeSecret(byte[] bytes) {
        if (bytes == null || bytes.length == 0) return List.of();
        TreeMap<String, String> kv = new TreeMap<>();
        TreeMap<String, String> typeOf = new TreeMap<>();
        try (ByteArrayInputStream in = new ByteArrayInputStream(bytes);
             ArrowStreamReader r = new ArrowStreamReader(in, Allocators.root())) {
            if (r.loadNextBatch()) {
                VectorSchemaRoot root = r.getVectorSchemaRoot();
                for (Field f : root.getSchema().getFields()) {
                    FieldVector v = root.getVector(f.getName());
                    if (v == null || v.isNull(0)) continue;
                    // Resolved secret arrives as a single struct field named
                    // after the secret type (e.g. {vgi_example: <struct>}).
                    // Descend into its children, skipping protocol fields
                    // that aren't user-supplied parameters.
                    if (v instanceof org.apache.arrow.vector.complex.StructVector sv) {
                        // Secrets are keyed by name; select the vgi_example-typed one.
                        FieldVector typeCol = sv.getChild("type");
                        String secretType = (typeCol != null && !typeCol.isNull(0))
                                ? String.valueOf(typeCol.getObject(0)) : "";
                        if (!"vgi_example".equals(secretType)) continue;
                        for (Field child : f.getChildren()) {
                            String name = child.getName();
                            if (PROTOCOL_FIELDS.contains(name)) continue;
                            FieldVector cv = sv.getChild(name);
                            if (cv == null || cv.isNull(0)) continue;
                            kv.put(name, String.valueOf(cv.getObject(0)));
                            typeOf.put(name, arrowTypeName(child));
                        }
                    } else {
                        kv.put(f.getName(), String.valueOf(v.getObject(0)));
                        typeOf.put(f.getName(), arrowTypeName(f));
                    }
                }
            }
        } catch (Exception ignore) {
            return List.of();
        }
        List<String[]> out = new ArrayList<>(kv.size());
        for (var e : kv.entrySet()) {
            out.add(new String[]{e.getKey(), e.getValue(), typeOf.get(e.getKey())});
        }
        return out;
    }

    /** Protocol-supplied fields on a resolved secret struct (not user data). */
    private static final java.util.Set<String> PROTOCOL_FIELDS =
            java.util.Set.of("name", "type", "provider", "scope");

    /** Friendly Arrow type name (Utf8 → "string", Int64 → "int64", etc.). */
    private static String arrowTypeName(Field f) {
        org.apache.arrow.vector.types.pojo.ArrowType t = f.getType();
        if (t instanceof org.apache.arrow.vector.types.pojo.ArrowType.Utf8) return "string";
        if (t instanceof org.apache.arrow.vector.types.pojo.ArrowType.Bool) return "bool";
        if (t instanceof org.apache.arrow.vector.types.pojo.ArrowType.Int i) return "int" + i.getBitWidth();
        if (t instanceof org.apache.arrow.vector.types.pojo.ArrowType.FloatingPoint) return "double";
        return t.toString();
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public List<String[]> rows;
        public boolean done;

        public State() {}
        State(List<String[]> rows) { this.rows = rows; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            if (rows.isEmpty()) { out.finish(); return; }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            VarCharVector kv = (VarCharVector) root.getVector("key");
            VarCharVector vv = (VarCharVector) root.getVector("value");
            VarCharVector tv = (VarCharVector) root.getVector("arrow_type");
            for (int i = 0; i < rows.size(); i++) {
                kv.setSafe(i, new Text(rows.get(i)[0]));
                vv.setSafe(i, new Text(rows.get(i)[1]));
                tv.setSafe(i, new Text(rows.get(i)[2]));
            }
            root.setRowCount(rows.size());
            out.emit(root);
            out.finish();
        }
    }
}
