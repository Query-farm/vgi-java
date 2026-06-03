// Copyright 2026 Query Farm LLC - https://query.farm

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;

/**
 * {@code exception_finalize(data TABLE)} — a buffering function that buffers
 * input normally but raises during the finalize (Source) phase. Mirrors
 * vgi-python's {@code ExceptionFinalizeFunction} (a {@code SumAllColumnsFunction}
 * subclass). Like the other buffering fixtures the error propagates from the
 * finalize producer on both the launcher and HTTP transports.
 */
public final class ExceptionFinalizeFunction extends SumAllColumnsBufferingFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("exception_finalize")
            .metadata(FunctionMetadata.describe("Test function that raises exception during finalize")
                    .withCategories("test", "error"))
            .table("data")
            .named("logging", Schemas.BOOL, "false")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new BufferingFinalizeProducer(params) {
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                throw new RuntimeException("Intentional exception during finalize()");
            }
        };
    }
}
