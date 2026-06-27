// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.copyto;

import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.table.CopyToFunction;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/**
 * Fixture {@code COPY ... TO} format writer for VGI integration tests. Mirrors
 * vgi-python's {@code ExampleLinesCopyToFunction}.
 *
 * <p>Registers the SQL format {@code example_lines_out} — a toy delimited-text
 * writer, the symmetric counterpart of the {@code example_lines} reader. It
 * exercises the COPY-TO Sink+Combine path plus the option machinery: a required
 * option ({@code null_string}), a defaulted option ({@code delimiter}), a BOOLEAN
 * option ({@code header}), and an enum/{@code choices} option ({@code on_exists}).
 *
 * <p>Shards are buffered in {@code params.storage()} ({@code execution_id}-scoped)
 * by {@link #write} and concatenated to the destination by {@link #close} — the
 * cross-process-safe pattern, so it works under pool rotation / HTTP.
 *
 * <pre>{@code
 *   COPY (SELECT * FROM t) TO '/path/out.txt'
 *     (FORMAT 'acme.example_lines_out', null_string 'NA');
 * }</pre>
 */
public class ExampleLinesCopyToFunction extends CopyToFunction {

    /** Append-log namespace holding one IPC blob per buffered input batch. */
    static final byte[] NS_SHARD = "copy_to_shard".getBytes(StandardCharsets.UTF_8);
    static final byte[] KEY = new byte[0];

    private static final FunctionSpec SPEC = new FunctionSpec(
            "example_lines_writer",
            FunctionMetadata.describe("Write the COPY source to a delimited text file")
                    .withCategories("copy", "test")
                    .withTags(Map.of("category", "copy_to", "stability", "test")),
            List.of(
                    // null_string is required (named, no default) — a missing
                    // value throws at write time naming the option.
                    new ArgSpec("null_string", -1, Schemas.UTF8, "Token written for SQL NULL",
                            /*isConst=*/true, /*hasDefault=*/false, "", List.of(),
                            false, false, false),
                    new ArgSpec("delimiter", -1, Schemas.UTF8, "Field separator",
                            true, true, ",", List.of(), false, false, false),
                    new ArgSpec("header", -1, Schemas.BOOL, "Write a header row of column names",
                            true, true, "false", List.of(), false, false, false),
                    new ArgSpec("on_exists", -1, Schemas.UTF8,
                            "Behavior when the destination file already exists",
                            true, true, "overwrite", List.of(), false, false, false)));

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public String copyToFormat() { return "example_lines_out"; }

    @Override public String copyToComment() { return "Toy delimited-text writer for tests"; }

    @Override
    public void write(VectorSchemaRoot batch, Arguments options, String filePath,
                      TableBufferingProcessParams params) {
        // state_append is atomic + race-safe across parallel sink threads/workers.
        params.storage().stateAppend(NS_SHARD, KEY, BatchUtil.writeSingleBatch(batch));
    }

    @Override
    public long close(Arguments options, String filePath, TableBufferingCombineParams params) {
        ParameterExtractor p = ParameterExtractor.of(options);
        String nullString = p.named("null_string").asString().required();
        String delimiter = p.named("delimiter").asString().orElse(",");
        boolean header = p.named("header").asBool().orElse(false);
        String onExists = p.named("on_exists").asString().oneOf("overwrite", "error").orElse("overwrite");

        Path dest = Path.of(filePath);
        if ("error".equals(onExists) && Files.exists(dest)) {
            throw new IllegalStateException(
                    "example_lines_out: destination already exists: " + filePath);
        }

        List<FunctionStorage.LogEntry> shards = params.storage().stateLogScan(NS_SHARD, KEY, -1L, 0);
        long rowsWritten = 0;
        try (BufferedWriter fh = Files.newBufferedWriter(dest, StandardCharsets.UTF_8)) {
            boolean wroteHeader = false;
            for (FunctionStorage.LogEntry e : shards) {
                try (VectorSchemaRoot root = BatchUtil.readSingleBatch(e.value(), Allocators.root())) {
                    if (header && !wroteHeader) {
                        fh.write(headerLine(root.getSchema(), delimiter));
                        wroteHeader = true;
                    }
                    int rows = root.getRowCount();
                    List<FieldVector> vectors = root.getFieldVectors();
                    for (int r = 0; r < rows; r++) {
                        StringBuilder line = new StringBuilder();
                        for (int c = 0; c < vectors.size(); c++) {
                            if (c > 0) line.append(delimiter);
                            line.append(fmt(vectors.get(c).getObject(r), nullString));
                        }
                        line.append('\n');
                        fh.write(line.toString());
                        rowsWritten++;
                    }
                }
            }
            // Empty COPY with header=true still emits the header row. The source
            // column names ride the bind's input schema.
            if (header && !wroteHeader && params.inputSchema() != null) {
                fh.write(headerLine(params.inputSchema(), delimiter));
            }
        } catch (IOException e) {
            throw new UncheckedIOException("example_lines_out: cannot write " + filePath, e);
        }
        return rowsWritten;
    }

    private static String headerLine(Schema schema, String delimiter) {
        StringBuilder sb = new StringBuilder();
        List<Field> fields = schema.getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) sb.append(delimiter);
            sb.append(fields.get(i).getName());
        }
        sb.append('\n');
        return sb.toString();
    }

    private static String fmt(Object value, String nullString) {
        return value == null ? nullString : value.toString();
    }
}
