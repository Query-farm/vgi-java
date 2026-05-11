// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.tableinout;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
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
 * {@code slow_cancellable_inout(sleep_ms BIGINT [const], probe_path VARCHAR [const],
 * data TABLE)} — sleeps {@code sleep_ms} per input batch then emits unchanged.
 * Used by table_in_out cancellation tests.
 */
public final class SlowCancellableInoutFunction extends PassthroughTIOFunction {

    @Override public String name() { return "slow_cancellable_inout"; }
    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Slow table-in-out with on_cancel probe (test fixture)");
    }
    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                ArgSpec.positional("sleep_ms", 0, Schemas.INT64),
                ArgSpec.positional("probe_path", 1, Schemas.UTF8),
                ArgSpec.table("data", 2));
    }

    @Override public TableInOutExchangeState createExchange(TableInOutInitParams params) {
        long sleepMs = ((Number) params.arguments().positionalAt(0)).longValue();
        String probePath = (String) params.arguments().positionalAt(1);
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
