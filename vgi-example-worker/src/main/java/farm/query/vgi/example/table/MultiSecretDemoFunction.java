// Copyright 2025, 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.Secrets;
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
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.Serializable;
import java.util.List;

/**
 * {@code multi_secret_demo(path STRING [const])} — resolves TWO same-type scoped
 * secrets in one bind, then selects the one matching the {@code path} argument via
 * {@link Secrets#forScopeOfType}. Emits one row: {@code (api_key)} carrying the
 * matched secret's {@code api_key} field (empty string when no scope matches).
 *
 * <p>It requests the {@code vgi_example} secret for both {@code s3://bucket-a/} and
 * {@code s3://bucket-b/} scopes in a single bind. Because resolved secrets are
 * keyed by name, both survive; {@code forScopeOfType} then picks the one whose
 * scope matches the path argument and returns its {@code api_key}.</p>
 */
public final class MultiSecretDemoFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            Schemas.nullable("api_key", Schemas.UTF8)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    private static final FunctionSpec SPEC = FunctionSpec.builder("multi_secret_demo")
            .description("Demo: two same-type scoped secrets resolved in one bind")
            .constArg("path", Schemas.UTF8)
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableBindParams p) {
        if (!p.resolvedSecretsProvided()) {
            // Phase-1: request the vgi_example secret for two distinct scopes.
            return new BindResponse(OUTPUT_SCHEMA_IPC, new byte[0],
                    List.of("vgi_example", "vgi_example"),
                    List.of("s3://bucket-a/", "s3://bucket-b/"),
                    List.of("", ""));
        }
        // Phase-2: secrets resolved, return output schema.
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        Object pathObj = params.arguments().positional().isEmpty()
                ? null : params.arguments().positionalAt(0);
        String path = pathObj == null ? "" : pathObj.toString();
        String apiKey = Secrets.parse(params.secrets())
                .forScopeOfType(path, "vgi_example")
                .map(m -> m.getOrDefault("api_key", ""))
                .orElse("");
        return new State(apiKey);
    }

    public static final class State extends TableProducerState implements Serializable {
        private static final long serialVersionUID = 1L;
        public String apiKey;
        public boolean done;

        public State() {}
        State(String apiKey) { this.apiKey = apiKey; }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            ((VarCharVector) root.getVector("api_key")).setSafe(0, new Text(apiKey));
            root.setRowCount(1);
            out.emit(root);
            out.finish();
        }
    }
}
