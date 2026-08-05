// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.protocol.FunctionExample;

import java.util.List;

/**
 * {@code global_buffered(data TABLE) -> *} — the table-buffering member of the
 * global-registration probe family (see
 * {@code farm.query.vgi.example.scalar.GlobalScalarFunction} for the family's
 * rationale).
 *
 * <p>Buffers every input batch and replays them on finalize; the output schema
 * is the input schema. Mirrors vgi-python's {@code GlobalBufferedFunction}.
 */
public final class GlobalBufferedFunction extends AbstractBufferAndDrain {

    private static final FunctionSpec SPEC = FunctionSpec.builder("global_buffered")
            .metadata(FunctionMetadata.describe("Global-registration probe (table-buffering)")
                    .withCategories("test", "global")
                    .withExamples(List.of(new FunctionExample(
                            "SELECT * FROM vgi_example_global_buffered((SELECT 1 AS x))",
                            "Buffering probe published into system.main", null))))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }
}
