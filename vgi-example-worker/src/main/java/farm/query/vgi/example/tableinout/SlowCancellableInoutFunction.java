// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.tableinout.TableInOutExchangeState;
import farm.query.vgi.tableinout.PassthroughTIOFunction;
import farm.query.vgi.tableinout.TableInOutFunction;
import farm.query.vgi.tableinout.TableInOutInitParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.AnnotatedBatch;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;

import java.util.List;

/**
 * {@code slow_cancellable_inout(probe_path VARCHAR [const], data TABLE, sleep_ms := 50)}
 * — sleeps {@code sleep_ms} per input batch then emits unchanged.
 * Used by table_in_out cancellation tests.
 */
public final class SlowCancellableInoutFunction extends PassthroughTIOFunction {

    private static final FunctionSpec SPEC = FunctionSpec.builder("slow_cancellable_inout")
            .description("Slow table-in-out with on_cancel probe (test fixture)")
            .constArg("probe_path", Schemas.UTF8)
            .table("data")
            .named("sleep_ms", Schemas.INT64, "50")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        String probePath = (String) params.arguments().positionalAt(0);
        long sleepMs = params.arguments().namedLong("sleep_ms", 50L);
        return new State(sleepMs, probePath);
    }

    public static final class State extends TableInOutExchangeState {
        public long sleepMs;
        public String probePath;
        public int processed;

        public State() {}
        State(long sleepMs, String probePath) { this.sleepMs = sleepMs; this.probePath = probePath; }

        @Override public void onInputBatch(AnnotatedBatch input, OutputCollector out, CallContext ctx) {
            if (sleepMs > 0) {
                try { Thread.sleep(sleepMs); }
                catch (InterruptedException e) { Thread.currentThread().interrupt(); }
            }
            processed++;
            out.emit(input.root());
        }
    }
}
