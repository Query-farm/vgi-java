// Copyright 2025-2026 Query.Farm LLC
// SPDX-License-Identifier: Apache-2.0

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableBindParams;
import farm.query.vgi.table.TableFunction;
import farm.query.vgi.table.TableInitParams;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.FileWriter;
import java.util.List;

/**
 * {@code slow_cancellable(probe_path [, sleep_ms := 50, count := 1_000_000])}
 * — slow producer that emits one row per tick (with a configurable sleep)
 * and writes a probe line to {@code probe_path} when {@code onCancel} fires.
 *
 * <p>Used to verify that DuckDB's pipeline-teardown signal reaches the
 * worker via the {@link farm.query.vgirpc.Metadata#CANCEL} tick metadata.</p>
 */
public final class SlowCancellableFunction implements TableFunction {

    private static final Schema OUTPUT_SCHEMA = new Schema(List.of(
            new Field("n", new FieldType(true, Schemas.INT64, null), null)));
    private static final byte[] OUTPUT_SCHEMA_IPC =
            farm.query.vgi.internal.SchemaUtil.serializeSchema(OUTPUT_SCHEMA);

    @Override public String name() { return "slow_cancellable"; }

    @Override public FunctionMetadata metadata() {
        return FunctionMetadata.describe("Slow producer with an on_cancel file-writing probe (test fixture)");
    }

    @Override public List<ArgSpec> argumentSpecs() {
        return List.of(
                new ArgSpec("probe_path", 0, Schemas.UTF8, /*isConst=*/true),
                new ArgSpec("sleep_ms", -1, Schemas.INT64, "", true, true, "50",
                        List.of(), false, false),
                new ArgSpec("count", -1, Schemas.INT64, "", true, true, "1000000",
                        List.of(), false, false));
    }

    @Override public BindResponse onBind(TableBindParams params) {
        return BindResponse.forSchema(OUTPUT_SCHEMA_IPC);
    }

    @Override public TableProducerState createProducer(TableInitParams params) {
        String probePath = (String) params.arguments().positionalAt(0);
        Object sleepObj = params.arguments().named().get("sleep_ms");
        long sleepMs = sleepObj == null ? 50L : ((Number) sleepObj).longValue();
        Object countObj = params.arguments().named().get("count");
        long count = countObj == null ? 1_000_000L : ((Number) countObj).longValue();
        return new State(probePath, sleepMs, count);
    }

    public static final class State extends TableProducerState {
        public String probePath;
        public long sleepMs;
        public long count;
        public long emitted;

        public State() {}

        State(String probePath, long sleepMs, long count) {
            this.probePath = probePath;
            this.sleepMs = sleepMs;
            this.count = count;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted >= count) {
                out.finish();
                return;
            }
            if (sleepMs > 0) {
                try { Thread.sleep(sleepMs); } catch (InterruptedException ignore) {}
            }
            VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT_SCHEMA, Allocators.root());
            root.allocateNew();
            BigIntVector v = (BigIntVector) root.getVector("n");
            v.setSafe(0, emitted);
            root.setRowCount(1);
            emitted++;
            out.emit(root);
        }

        @Override public void onCancel(CallContext ctx) {
            try (FileWriter w = new FileWriter(probePath, true)) {
                w.write("pid=" + ProcessHandle.current().pid() + " emitted=" + emitted + "\n");
            } catch (Exception ignore) {}
        }
    }
}
