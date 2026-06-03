// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.types.Schemas;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.nio.charset.StandardCharsets;

/**
 * {@code exception_process(data TABLE)} — a buffering function that raises on
 * every second {@code process()} call. Mirrors vgi-python's
 * {@code ExceptionProcessFunction} (a {@code SumAllColumnsFunction} subclass).
 *
 * <p>The batch counter is a race-safe append-only log keyed by
 * {@code execution_id}: {@code process()} appends one marker and counts the
 * log, raising when the count is even. It deliberately does <em>not</em>
 * accumulate into the sum log, so a run that never trips the rule finalizes to
 * a clean zero-sum row (the test contract). Being a buffering Sink+Source
 * function, the counter lives in worker storage and survives the stateless
 * HTTP process round-trip — unlike the old streaming table-in-out shape, whose
 * per-exchange state never reached the finalize phase over HTTP.
 */
public final class ExceptionProcessFunction extends SumAllColumnsBufferingFunction {

    private static final byte[] NS_COUNT = "count".getBytes(StandardCharsets.UTF_8);

    private static final FunctionSpec SPEC = FunctionSpec.builder("exception_process")
            .metadata(FunctionMetadata.describe("Test function that raises exception during process")
                    .withCategories("test", "error"))
            .table("data")
            .named("logging", Schemas.BOOL, "false")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        params.storage().stateAppend(NS_COUNT, KEY, new byte[0]);
        int count = params.storage().stateLogScan(NS_COUNT, KEY, -1, Integer.MAX_VALUE).size();
        if (count % 2 == 0) {
            throw new RuntimeException("Intentional exception on batch " + count);
        }
        return params.executionId();
    }
}
