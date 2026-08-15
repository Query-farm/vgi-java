// VGI-Java example: a table-buffering (Sink + Source) function.
//
// A buffering function must see ALL input before it produces ANY output — think
// sort, top-k, or whole-relation aggregation. Unlike a TIO function (which emits
// per input batch), it has a three-phase lifecycle:
//
//   process()                — Sink: stash each input batch, return a state_id
//   combine()                — once at end-of-input: group state_ids into the
//                              finalize streams the Source will drain
//   createFinalizeProducer() — Source: emit the buffered rows back out
//
// State is stashed in `params.storage()` — a durable, execution-scoped key/value
// + append-log store — so buffering survives even when DuckDB spreads the Sink
// across parallel workers. This example is the canonical "collect every batch,
// replay it during finalize" (a passthrough that happens to fully buffer).
//
//   ATTACH 'demo' AS demo (TYPE vgi, LOCATION 'launch:/abs/path/bin/runBuffering');
//   SELECT * FROM demo.collect((SELECT * FROM range(3) t(x)));   -- 0,1,2
package farm.query.vgi.examples;

import farm.query.vgi.Worker;
import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.BatchUtil;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.storage.FunctionStorage;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** {@code collect(data TABLE) -> *}: buffers every input batch, replays them. */
public final class BufferingExample implements TableBufferingFunction {

    // A namespace + key naming the append-log we buffer batches into.
    private static final byte[] NS = "buf".getBytes(StandardCharsets.UTF_8);
    private static final byte[] KEY = new byte[0];

    private static final FunctionSpec SPEC = FunctionSpec.builder("collect")
            .metadata(FunctionMetadata.describe("Buffer all input, then replay it")
                    .withCategories("utility"))
            .table("data")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    // Output schema = input schema (passthrough).
    @Override public BindResponse onBind(TableInOutBindParams params) {
        Schema in = params.inputSchema();
        Schema out = (in == null || in.getFields().isEmpty()) ? new Schema(List.of()) : in;
        return BindResponse.forSchema(SchemaUtil.serializeSchema(out));
    }

    // Sink: append this batch's IPC bytes to the log, return our execution id as
    // the state_id (every batch of one execution shares the same log).
    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        params.storage().stateAppend(NS, KEY, BatchUtil.writeSingleBatch(batch));
        return params.executionId();
    }

    // Combine: one output stream, keyed by the execution id.
    @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        return List.of(params.executionId());
    }

    // Source: drain the log one buffered batch per tick.
    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        return new ReplayProducer(params);
    }

    private static final class ReplayProducer extends BufferingFinalizeProducer {
        private long afterId = -1;   // log cursor; -1 = before the first entry

        ReplayProducer(TableBufferingFinalizeParams params) { super(params); }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            List<FunctionStorage.LogEntry> rows = storage().stateLogScan(NS, KEY, afterId, 1);
            if (rows.isEmpty()) { out.finish(); return; }
            FunctionStorage.LogEntry e = rows.get(0);
            VectorSchemaRoot full = BatchUtil.readSingleBatch(e.value(), Allocators.root());
            emitProjected(full, out);   // narrows to projected cols + applies filters
            full.close();
            afterId = e.id();
        }
    }

    public static void main(String[] args) {
        Worker.builder()
                .catalogName("demo")
                .registerTableBuffering(new BufferingExample())
                .runFromArgs(args);
    }
}
