// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.copyto;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.types.Schemas;

import java.util.List;
import java.util.Map;

/**
 * Ordered variant of {@link ExampleLinesCopyToFunction}. Mirrors vgi-python's
 * {@code ExampleLinesOrderedCopyToFunction}.
 *
 * <p>Overriding {@link #sinkOrderDependent()} to {@code true} makes the extension
 * use a single-threaded sink ({@code REGULAR_COPY_TO_FILE}), so the worker
 * receives every batch in source order and writes the file in order. Discovery
 * surfaces this as {@code ordered=true} in {@code vgi_copy_formats()}.
 */
public final class ExampleLinesOrderedCopyToFunction extends ExampleLinesCopyToFunction {

    private static final FunctionSpec SPEC = new FunctionSpec(
            "example_lines_ordered_writer",
            FunctionMetadata.describe("Write the COPY source to a delimited file, preserving source order")
                    .withCategories("copy", "test")
                    .withTags(Map.of("category", "copy_to", "stability", "test")),
            List.of(
                    new ArgSpec("null_string", -1, Schemas.UTF8, "Token written for SQL NULL",
                            true, false, "", List.of(), false, false, false),
                    new ArgSpec("delimiter", -1, Schemas.UTF8, "Field separator",
                            true, true, ",", List.of(), false, false, false),
                    new ArgSpec("header", -1, Schemas.BOOL, "Write a header row of column names",
                            true, true, "false", List.of(), false, false, false),
                    new ArgSpec("on_exists", -1, Schemas.UTF8,
                            "Behavior when the destination file already exists",
                            true, true, "overwrite", List.of(), false, false, false)));

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public String copyToFormat() { return "example_lines_ordered_out"; }

    @Override public String copyToComment() {
        return "Toy delimited-text writer (ordered, single-thread sink)";
    }

    @Override public boolean sinkOrderDependent() { return true; }
}
