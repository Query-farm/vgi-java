// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.copyfrom;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.function.ParameterExtractor;
import farm.query.vgi.table.CopyFromFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Fixture {@code COPY ... FROM} format reader for VGI integration tests. Mirrors
 * vgi-python's {@code ExampleLinesCopyFromFunction}.
 *
 * <p>Registers the SQL format {@code example_lines} — a toy delimited-text
 * reader. It exercises the full COPY-FROM path plus the option machinery: a
 * defaulted option ({@code delimiter}), an {@code INTEGER}-valued option with a
 * range constraint ({@code skip_rows}), a required option ({@code null_string}),
 * and an enum/{@code choices} option ({@code on_error}).
 *
 * <pre>{@code
 *   CREATE TABLE t (a INTEGER, b VARCHAR);
 *   COPY t FROM '/path/data.txt' (FORMAT example_lines, null_string 'NA');
 * }</pre>
 */
public final class ExampleLinesCopyFromFunction extends CopyFromFunction {

    private static final FunctionSpec SPEC = new FunctionSpec(
            "example_lines_copy_reader",
            FunctionMetadata.describe("Read a delimited text file into the COPY target table")
                    .withCategories("copy", "test")
                    .withTags(Map.of("category", "copy_from", "stability", "test")),
            List.of(
                    // null_string is required (named, no default) — a missing
                    // value throws at read time with the option name in the
                    // message (worker-side enforcement).
                    new ArgSpec("null_string", -1, Schemas.UTF8, "Token parsed as SQL NULL",
                            /*isConst=*/true, /*hasDefault=*/false, "", List.of(),
                            false, false, false),
                    new ArgSpec("delimiter", -1, Schemas.UTF8, "Field separator",
                            true, true, ",", List.of(), false, false, false),
                    new ArgSpec("skip_rows", -1, Schemas.INT64, "Leading lines to skip before data",
                            true, true, "0", List.of(), false, false, false),
                    new ArgSpec("on_error", -1, Schemas.UTF8,
                            "Behavior on a row whose column count does not match the target",
                            true, true, "fail", List.of(), false, false, false)));

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public String copyFromFormat() { return "example_lines"; }

    @Override public String copyFromComment() { return "Toy delimited-text reader for tests"; }

    @Override
    public void read(String path, Arguments options, Schema expectedSchema,
                      TableInitParams params, Emitter out, CallContext ctx) {
        ParameterExtractor p = ParameterExtractor.of(options);
        // required(): throws IllegalArgumentException naming "null_string" when absent.
        String nullString = p.named("null_string").asString().required();
        String delimiter = p.named("delimiter").asString().orElse(",");
        long skipRows = p.named("skip_rows").asLong().ge(0).orElse(0L);
        String onError = p.named("on_error").asString().oneOf("fail", "skip").orElse("fail");

        List<String> lines;
        try {
            lines = Files.readAllLines(Path.of(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("example_lines: cannot read " + path, e);
        }

        int ncols = expectedSchema.getFields().size();
        Pattern split = Pattern.compile(Pattern.quote(delimiter));
        List<String[]> rows = new ArrayList<>();
        for (int idx = (int) Math.min(skipRows, lines.size()); idx < lines.size(); idx++) {
            String line = lines.get(idx);
            if (line.isEmpty()) continue;
            String[] cells = split.split(line, -1);
            if (cells.length != ncols) {
                if ("skip".equals(onError)) continue;
                throw new IllegalArgumentException(
                        "example_lines: row has " + cells.length + " fields, expected "
                                + ncols + ": " + line);
            }
            rows.add(cells);
        }

        VectorSchemaRoot root = VectorSchemaRoot.create(expectedSchema, params.allocator());
        root.allocateNew();
        for (int c = 0; c < ncols; c++) {
            FieldVector v = root.getVector(c);
            for (int r = 0; r < rows.size(); r++) {
                String cell = rows.get(r)[c];
                if (cell.equals(nullString)) {
                    v.setNull(r);
                } else {
                    setCell(v, r, cell, expectedSchema.getFields().get(c));
                }
            }
        }
        root.setRowCount(rows.size());
        out.emit(root);
    }

    /** Parse one string cell into {@code v} at {@code row}, coercing to the target type. */
    private static void setCell(FieldVector v, int row, String cell, Field field) {
        ArrowType t = field.getType();
        if (t instanceof ArrowType.Utf8 || t instanceof ArrowType.LargeUtf8) {
            ((VarCharVector) v).setSafe(row, new Text(cell));
        } else if (t instanceof ArrowType.Int i && i.getBitWidth() == 32) {
            ((IntVector) v).setSafe(row, Integer.parseInt(cell.trim()));
        } else if (t instanceof ArrowType.Int i && i.getBitWidth() == 64) {
            ((BigIntVector) v).setSafe(row, Long.parseLong(cell.trim()));
        } else if (t instanceof ArrowType.FloatingPoint fp
                && fp.getPrecision() == FloatingPointPrecision.DOUBLE) {
            ((Float8Vector) v).setSafe(row, Double.parseDouble(cell.trim()));
        } else if (t instanceof ArrowType.FloatingPoint fp
                && fp.getPrecision() == FloatingPointPrecision.SINGLE) {
            ((Float4Vector) v).setSafe(row, Float.parseFloat(cell.trim()));
        } else if (t instanceof ArrowType.Bool) {
            ((BitVector) v).setSafe(row, Boolean.parseBoolean(cell.trim()) ? 1 : 0);
        } else {
            throw new IllegalArgumentException(
                    "example_lines: unsupported target type " + t + " for column " + field.getName());
        }
    }
}
