// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.copyfrom;

import farm.query.vgi.Secrets;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.table.CopyFromFunction;
import farm.query.vgi.table.CopySecretLookup;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.util.List;
import java.util.Map;

/**
 * Fixture {@code COPY ... FROM} reader that forwards a {@code CREATE SECRET}
 * credential. Mirrors vgi-python's {@code SecretLinesCopyFromFunction}.
 *
 * <p>Exercises the COPY-FROM secret-bind hook ({@link #secretLookups}): it
 * requests the {@code secret_type} secret scoped to the source path during bind,
 * and {@link #read} emits a single VARCHAR row holding the resolved secret's
 * {@code api_key} (or {@code NONE}) — so a test can assert the caller's secret
 * reached the reader.
 */
public final class SecretLinesCopyFromFunction extends CopyFromFunction {

    private static final FunctionSpec SPEC = new FunctionSpec(
            "secret_lines_reader",
            FunctionMetadata.describe("Emit the resolved secret's api_key as a single VARCHAR row")
                    .withCategories("copy", "test", "secret")
                    .withTags(Map.of("category", "copy_from", "stability", "test")),
            List.of(new ArgSpec("secret_type", -1, Schemas.UTF8,
                    "Secret type to fetch, scoped by the source path",
                    /*isConst=*/true, /*hasDefault=*/true, "vgi_example", List.of(),
                    false, false, false)));

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public String copyFromFormat() { return "secret_lines_in"; }

    @Override public String copyFromComment() {
        return "Reader that forwards a CREATE SECRET credential (test fixture)";
    }

    private static String secretType(Arguments options) {
        return ParameterExtractor.of(options).named("secret_type").asString().orElse("vgi_example");
    }

    @Override
    public List<CopySecretLookup> secretLookups(TableBindParams params) {
        // Request the source-scoped secret; the framework's two-phase secret bind
        // resolves it and surfaces it on params.secrets() at read time.
        if (params.copyFrom() == null) {
            return List.of();
        }
        return List.of(CopySecretLookup.scoped(secretType(params.arguments()),
                params.copyFrom().file_path()));
    }

    @Override
    public void read(String path, Arguments options, Schema expectedSchema,
                      TableInitParams params, Emitter out, CallContext ctx) {
        String type = secretType(options);
        String apiKey = Secrets.parse(params.secrets())
                .forScopeOfType(path, type)
                .map(m -> m.get("api_key"))
                .orElse("NONE");

        if (expectedSchema.getFields().size() != 1) {
            throw new IllegalArgumentException(
                    "secret_lines_in: expected a single-column target, got "
                            + expectedSchema.getFields().size());
        }
        VectorSchemaRoot root = VectorSchemaRoot.create(expectedSchema, params.allocator());
        root.allocateNew();
        ((VarCharVector) root.getVector(0)).setSafe(0, new Text(apiKey));
        root.setRowCount(1);
        out.emit(root);
    }
}
