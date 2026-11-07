// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.table;

import farm.query.vgi.Secrets;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.protocol.FunctionRequiredSecret;
import farm.query.vgi.table.SimpleTableFunction;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Secret-lookup conformance fixtures: the five table functions the shared
 * {@code test/sql/integration/secret/secret_lookup_*.test} files run against
 * every SDK's example worker. The contract (names, declared requirements,
 * output columns, rendering) is the vgi repo's
 * {@code docs/secret_lookup_fixtures.md}; this mirrors vgi-python's
 * {@code vgi/_test_fixtures/secret_lookup.py}.
 *
 * <p>Every function reads {@code vgi_example} secrets and selects them through
 * the public {@link Secrets} selectors (by type / by name / by scope), never by
 * indexing the batch's columns: the columns are keyed by secret name.</p>
 */
public final class SecretLookupFixtures {

    private SecretLookupFixtures() {}

    /** The secret type every value fixture reads (registered by the example worker). */
    public static final String SECRET_TYPE = "vgi_example";
    /** Outer static scope of {@code secret_value_for_scope}. */
    public static final String SCOPE_A = "vgi-conformance://a/";
    /** Inner static scope of {@code secret_value_for_scope}. */
    public static final String SCOPE_AB = "vgi-conformance://a/b/";
    /** Scope of the {@code s3} requirement of {@code secret_types_seen}. */
    public static final String S3_SCOPE = "s3://vgi-conformance/";

    private static final Schema VALUE_SCHEMA = Schemas.of(
            Schemas.nullable("found", Schemas.BOOL),
            Schemas.nullable("secret_name", Schemas.UTF8),
            Schemas.nullable("value", Schemas.UTF8));
    private static final byte[] VALUE_SCHEMA_IPC = SchemaUtil.serializeSchema(VALUE_SCHEMA);

    private static final Schema SEEN_SCHEMA = Schemas.of(
            Schemas.nullable("secret_name", Schemas.UTF8),
            Schemas.nullable("secret_type", Schemas.UTF8),
            Schemas.nullable("scope", Schemas.UTF8));

    /** Every fixture, for registration in the example catalog. */
    public static List<TableFunction> functions() {
        return List.of(new ByType(), new ByName(), new ForScope(), new Required(), new TypesSeen());
    }

    private static String arg(TableInitParams p, int i) {
        Object o = p.arguments().positional().size() > i ? p.arguments().positionalAt(i) : null;
        return o == null ? null : o.toString();
    }

    private static String arg(TableBindParams p, int i) {
        Object o = p.arguments().positional().size() > i ? p.arguments().positionalAt(i) : null;
        return o == null ? null : o.toString();
    }

    private static FunctionSpec.Builder specBuilder(String name, String description) {
        return FunctionSpec.builder(name)
                .metadata(FunctionMetadata.describe(description).withCategories("secret", "testing"));
    }

    /** The output row for a selected secret (or its absence). */
    static ValueState stateFor(Map<String, String> secret, String field) {
        if (secret == null) return new ValueState();
        ValueState s = new ValueState();
        s.found = true;
        s.secretName = secret.get("name");
        s.value = field == null ? null : secret.get(field);
        return s;
    }

    /** One {@code (found, secret_name, value)} row. Public fields for the HTTP state token. */
    public static final class ValueState extends TableProducerState {
        /** Whether a secret was selected. */
        public boolean found;
        /** The selected secret's name, or null. */
        public String secretName;
        /** The selected field rendered as text, or null. */
        public String value;
        /** Whether the row has been emitted. */
        public boolean done;

        /** Required no-arg constructor for state deserialization. */
        public ValueState() {}

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done) { out.finish(); return; }
            done = true;
            boolean f = found;
            String n = secretName;
            String v = value;
            BatchUtil.emit(VALUE_SCHEMA, 1, out, (root, rows, ignored) -> {
                ((BitVector) root.getVector("found")).setSafe(0, f ? 1 : 0);
                VarCharVector nv = (VarCharVector) root.getVector("secret_name");
                if (n == null) nv.setNull(0); else nv.setSafe(0, new Text(n));
                VarCharVector vv = (VarCharVector) root.getVector("value");
                if (v == null) vv.setNull(0); else vv.setSafe(0, new Text(v));
            });
        }
    }

    /** {@code secret_value_by_type(field)}: static simplest-form requirement, selected by type. */
    public static final class ByType extends SimpleTableFunction {
        private static final FunctionSpec SPEC = specBuilder("secret_value_by_type",
                "Conformance: a field of the vgi_example secret, selected by type")
                .arg("field", Schemas.UTF8).build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return VALUE_SCHEMA; }
        @Override public List<FunctionRequiredSecret> requiredSecrets() {
            return List.of(FunctionRequiredSecret.ofType(SECRET_TYPE));
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return stateFor(Secrets.parse(p.secrets()).lookup(SECRET_TYPE).orElse(null), arg(p, 0));
        }
    }

    /** {@code secret_value_required(field)}: as by-type, but the bind fails when absent. */
    public static final class Required implements TableFunction {
        private static final FunctionSpec SPEC = specBuilder("secret_value_required",
                "Conformance: a field of the required vgi_example secret; errors when absent")
                .arg("field", Schemas.UTF8).build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public List<FunctionRequiredSecret> requiredSecrets() {
            return List.of(FunctionRequiredSecret.ofType(SECRET_TYPE));
        }
        @Override public BindResponse onBind(TableBindParams p) {
            Secrets.parse(p.secrets()).require(SECRET_TYPE);
            return BindResponse.forSchema(VALUE_SCHEMA_IPC);
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return stateFor(Secrets.parse(p.secrets()).require(SECRET_TYPE), arg(p, 0));
        }
    }

    /** {@code secret_value_by_name(name, field)}: requested by name in a two-phase bind. */
    public static final class ByName implements TableFunction {
        private static final FunctionSpec SPEC = specBuilder("secret_value_by_name",
                "Conformance: a field of the named vgi_example secret")
                .arg("name", Schemas.UTF8).arg("field", Schemas.UTF8).build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override public BindResponse onBind(TableBindParams p) {
            String name = arg(p, 0);
            if (!p.resolvedSecretsProvided() && name != null) {
                return new BindResponse(VALUE_SCHEMA_IPC, new byte[0],
                        List.of(SECRET_TYPE), List.of(""), List.of(name));
            }
            return BindResponse.forSchema(VALUE_SCHEMA_IPC);
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            String name = arg(p, 0);
            Map<String, String> secret = name == null ? null
                    : Secrets.parse(p.secrets()).lookupNamed(SECRET_TYPE, name).orElse(null);
            return stateFor(secret, arg(p, 1));
        }
    }

    /** {@code secret_value_for_scope(path, field)}: three static same-type requirements, longest prefix. */
    public static final class ForScope extends SimpleTableFunction {
        private static final FunctionSpec SPEC = specBuilder("secret_value_for_scope",
                "Conformance: a field of the vgi_example secret whose scope best matches a path")
                .arg("path", Schemas.UTF8).arg("field", Schemas.UTF8).build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return VALUE_SCHEMA; }
        @Override public List<FunctionRequiredSecret> requiredSecrets() {
            return List.of(
                    FunctionRequiredSecret.ofType(SECRET_TYPE),
                    FunctionRequiredSecret.scoped(SECRET_TYPE, SCOPE_A),
                    FunctionRequiredSecret.scoped(SECRET_TYPE, SCOPE_AB));
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            String path = arg(p, 0);
            Map<String, String> secret = path == null ? null
                    : Secrets.parse(p.secrets()).forScopeOfType(path, SECRET_TYPE).orElse(null);
            return stateFor(secret, arg(p, 1));
        }
    }

    /** {@code secret_types_seen()}: requires vgi_example and scoped s3; one row per delivered secret. */
    public static final class TypesSeen extends SimpleTableFunction {
        private static final FunctionSpec SPEC = specBuilder("secret_types_seen",
                "Conformance: the secrets delivered for a vgi_example + s3 requirement").build();

        @Override public FunctionSpec spec() { return SPEC; }
        @Override protected Schema outputSchema() { return SEEN_SCHEMA; }
        @Override public List<FunctionRequiredSecret> requiredSecrets() {
            return List.of(
                    FunctionRequiredSecret.ofType(SECRET_TYPE),
                    FunctionRequiredSecret.scoped("s3", S3_SCOPE));
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            Secrets secrets = Secrets.parse(p.secrets());
            SeenState s = new SeenState();
            for (String name : new java.util.TreeSet<>(secrets.byName().keySet())) {
                s.names.add(name);
                s.types.add(secrets.secretType(name).orElse(null));
                s.scopes.add(secrets.namedField(name, "scope").orElse(""));
            }
            return s;
        }
    }

    /** One {@code (secret_name, secret_type, scope)} row per delivered secret. */
    public static final class SeenState extends TableProducerState {
        /** Delivered secret names. */
        public List<String> names = new ArrayList<>();
        /** Their {@code type} fields. */
        public List<String> types = new ArrayList<>();
        /** Their {@code scope} fields. */
        public List<String> scopes = new ArrayList<>();
        /** Whether the rows have been emitted. */
        public boolean done;

        /** Required no-arg constructor for state deserialization. */
        public SeenState() {}

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (done || names.isEmpty()) { out.finish(); return; }
            done = true;
            List<String> n = names, t = types, sc = scopes;
            BatchUtil.emit(SEEN_SCHEMA, n.size(), out, (root, rows, ignored) -> {
                VarCharVector nv = (VarCharVector) root.getVector("secret_name");
                VarCharVector tv = (VarCharVector) root.getVector("secret_type");
                VarCharVector sv = (VarCharVector) root.getVector("scope");
                for (int i = 0; i < rows; i++) {
                    nv.setSafe(i, new Text(n.get(i)));
                    if (t.get(i) == null) tv.setNull(i); else tv.setSafe(i, new Text(t.get(i)));
                    sv.setSafe(i, new Text(sc.get(i) == null ? "" : sc.get(i)));
                }
            });
        }
    }
}
