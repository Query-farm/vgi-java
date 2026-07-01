// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.tableinout;

import farm.query.vgi.Secrets;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * {@code secret_in_out(data TABLE) -> * + secret_string} — resolves a
 * {@code vgi_example} secret in a two-phase {@code onBind} and appends its
 * {@code secret_string} value as a constant column on every input row.
 *
 * <p>Exercises the intersection of the two-phase secret bind with a
 * table-in-out function's INPUT stream: the bind must retry with resolved
 * secrets AND preserve the input schema, then the resolved secret must reach
 * {@code process()} time (delivered via {@link TableInOutInitParams#secrets()})
 * to populate the appended column. Mirrors vgi-python's
 * {@code SecretInOutFunction}.
 */
public final class SecretInOutFunction implements TableInOutFunction {

    @Override public String name() { return "secret_in_out"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Append a resolved secret value to each input row")
                .withCategories("transform", "secret");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(ArgSpec.table("data", 0));
    }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        // Catalog enumeration: no input schema — return an empty schema.
        if (in == null) {
            return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(List.of())));
        }
        // Phase 1: request the vgi_example secret. DuckDB re-issues bind with
        // resolved_secrets_provided=true (or skips if none in scope).
        if (!params.resolvedSecretsProvided()) {
            return new BindResponse(SchemaUtil.serializeSchema(in), new byte[0],
                    List.of("vgi_example"), List.of(""), List.of(""));
        }
        // Phase 2: output = input schema + a Utf8 secret_string column.
        List<Field> fields = new ArrayList<>(in.getFields());
        fields.add(Schemas.nullable("secret_string", Schemas.UTF8));
        return BindResponse.forSchema(SchemaUtil.serializeSchema(new Schema(fields)));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        return new SecretInOutState(params.outputSchema(), resolveSecretString(params.secrets()));
    }

    /** Read the resolved vgi_example secret's secret_string once, at init. */
    private static String resolveSecretString(byte[] secretsIpc) {
        List<Map<String, String>> matches = Secrets.parse(secretsIpc).ofType("vgi_example");
        if (matches.isEmpty()) return null;
        return matches.get(0).get("secret_string");
    }

    /** Appends a constant secret_string column to each input batch (1:1 rows). */
    public static final class SecretInOutState extends TableInOutExchangeState {
        private final Schema outputSchema;
        private final String secretString;

        public SecretInOutState() { this.outputSchema = null; this.secretString = null; }

        public SecretInOutState(Schema outputSchema, String secretString) {
            this.outputSchema = outputSchema;
            this.secretString = secretString;
        }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            VectorSchemaRoot in = input.root();
            int rows = in.getRowCount();
            List<FieldVector> outVectors = new ArrayList<>();
            for (Field f : outputSchema.getFields()) {
                if ("secret_string".equals(f.getName())) {
                    VarCharVector sv = (VarCharVector) f.createVector(farm.query.vgirpc.wire.Allocators.root());
                    sv.allocateNew(rows);
                    for (int i = 0; i < rows; i++) {
                        if (secretString == null) sv.setNull(i);
                        else sv.setSafe(i, new Text(secretString));
                    }
                    sv.setValueCount(rows);
                    outVectors.add(sv);
                } else {
                    FieldVector src = (FieldVector) in.getVector(f.getName());
                    org.apache.arrow.vector.util.TransferPair tp =
                            src.getTransferPair(farm.query.vgirpc.wire.Allocators.root());
                    tp.transfer();
                    outVectors.add((FieldVector) tp.getTo());
                }
            }
            VectorSchemaRoot copy = new VectorSchemaRoot(outVectors);
            copy.setRowCount(rows);
            out.emit(copy);
        }
    }
}
