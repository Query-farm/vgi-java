// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.copyto;

import farm.query.vgi.Secrets;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.table.CopySecretLookup;
import farm.query.vgi.table.CopyToFunction;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Fixture {@code COPY ... TO} writer that forwards a {@code CREATE SECRET}
 * credential. Mirrors vgi-python's {@code SecretLinesCopyToFunction}.
 *
 * <p>Exercises the COPY-TO secret-bind hook ({@link #secretLookups}): it requests
 * the {@code secret_type} secret scoped to the destination path during bind, and
 * {@link #close} writes the resolved secret's {@code api_key} (or {@code NONE})
 * plus the row count — so a test can assert the caller's secret reached the writer
 * for a secret-backed cloud write.
 */
public final class SecretLinesCopyToFunction extends CopyToFunction {

    private static final byte[] NS_SHARD = "copy_to_secret_shard".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY = new byte[0];

    private static final FunctionSpec SPEC = new FunctionSpec(
            "secret_lines_writer",
            FunctionMetadata.describe("Write the resolved secret's api_key + row count to the destination")
                    .withCategories("copy", "test", "secret")
                    .withTags(Map.of("category", "copy_to", "stability", "test")),
            List.of(new ArgSpec("secret_type", -1, Schemas.UTF8,
                    "Secret type to fetch, scoped by the destination path",
                    /*isConst=*/true, /*hasDefault=*/true, "vgi_example", List.of(),
                    false, false, false)));

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public String copyToFormat() { return "secret_lines_out"; }

    @Override public String copyToComment() {
        return "Writer that forwards a CREATE SECRET credential (test fixture)";
    }

    private static String secretType(Arguments options) {
        return ParameterExtractor.of(options).named("secret_type").asString().orElse("vgi_example");
    }

    @Override
    public List<CopySecretLookup> secretLookups(TableInOutBindParams params) {
        // Request the destination-scoped secret; the framework's two-phase secret
        // bind resolves it and surfaces it on params.secrets() at close time.
        if (params.copyTo() == null) {
            return List.of();
        }
        return List.of(CopySecretLookup.scoped(secretType(params.arguments()), params.copyTo().file_path()));
    }

    @Override
    public void write(VectorSchemaRoot batch, Arguments options, String filePath,
                      TableBufferingProcessParams params) {
        // Record this shard's row count (cross-process-safe append).
        params.storage().stateAppend(NS_SHARD, KEY,
                Integer.toString(batch.getRowCount()).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public long close(Arguments options, String filePath, TableBufferingCombineParams params) {
        String type = secretType(options);
        String apiKey = Secrets.parse(params.secrets())
                .forScopeOfType(filePath, type)
                .map(m -> m.get("api_key"))
                .orElse("NONE");

        List<FunctionStorage.LogEntry> shards = params.storage().stateLogScan(NS_SHARD, KEY, -1L, 0);
        long total = 0;
        for (FunctionStorage.LogEntry e : shards) {
            total += Long.parseLong(new String(e.value(), StandardCharsets.UTF_8).trim());
        }

        Path dest = Path.of(filePath);
        try (BufferedWriter fh = Files.newBufferedWriter(dest, StandardCharsets.UTF_8)) {
            fh.write("api_key=" + apiKey + "\nrows=" + total + "\n");
        } catch (IOException e) {
            throw new UncheckedIOException("secret_lines_out: cannot write " + filePath, e);
        }
        return total;
    }
}
