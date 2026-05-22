// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.buffering;

import farm.query.vgi.buffering.BufferingFinalizeProducer;
import farm.query.vgi.buffering.TableBufferingCombineParams;
import farm.query.vgi.buffering.TableBufferingFinalizeParams;
import farm.query.vgi.buffering.TableBufferingFunction;
import farm.query.vgi.buffering.TableBufferingProcessParams;
import farm.query.vgi.function.Arguments;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionSpec;
import farm.query.vgi.internal.SchemaUtil;
import farm.query.vgi.protocol.BindResponse;
import farm.query.vgi.table.TableProducerState;
import farm.query.vgi.tableinout.TableInOutBindParams;
import farm.query.vgi.types.Schemas;
import farm.query.vgirpc.CallContext;
import farm.query.vgirpc.OutputCollector;
import farm.query.vgirpc.wire.Allocators;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * {@code slow_cancellable_buffering(probe_path, data TABLE, sleep_ms:=, count:=)}
 * — slow buffered producer used to exercise the Source-side cancel path. The
 * Sink absorbs (ignores) input; finalize emits {@code count} single-column rows
 * with a per-row sleep so a {@code LIMIT} query cancels before EOS. Mirrors
 * vgi-python {@code SlowCancellableBufferingFunction}.
 */
public final class SlowCancellableBufferingFunction implements TableBufferingFunction {

    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("n", Schemas.INT64));

    private static final FunctionSpec SPEC = FunctionSpec.builder("slow_cancellable_buffering")
            .metadata(FunctionMetadata.describe(
                    "Slow buffered table function with an on_cancel file probe (test fixture)")
                    .withCategories("test"))
            .constArg("probe_path", Schemas.UTF8)
            .table("data")
            .named("count", Schemas.INT64, "1000")
            .named("sleep_ms", Schemas.INT64, "20")
            .build();

    @Override public FunctionSpec spec() { return SPEC; }

    @Override public BindResponse onBind(TableInOutBindParams params) {
        return BindResponse.forSchema(SchemaUtil.serializeSchema(OUTPUT));
    }

    @Override public byte[] process(VectorSchemaRoot batch, TableBufferingProcessParams params) {
        return params.executionId();  // sink absorbs; input ignored
    }

    @Override public List<byte[]> combine(List<byte[]> stateIds, TableBufferingCombineParams params) {
        return List.of(params.executionId());
    }

    @Override public TableProducerState createFinalizeProducer(TableBufferingFinalizeParams params) {
        Arguments args = params.initParams().arguments();
        return new SlowProducer(params, args.namedLong("sleep_ms", 20), args.namedLong("count", 1000));
    }

    private static final class SlowProducer extends BufferingFinalizeProducer {
        private final long sleepMs;
        private final long total;
        private long emitted = 0;

        SlowProducer(TableBufferingFinalizeParams params, long sleepMs, long total) {
            super(params);
            this.sleepMs = sleepMs;
            this.total = total;
        }

        @Override public void produceTick(OutputCollector out, CallContext ctx) {
            if (emitted >= total) { out.finish(); return; }
            if (sleepMs > 0) {
                try { Thread.sleep(sleepMs); } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    out.finish();
                    return;
                }
            }
            Schema schema = outputSchema != null ? outputSchema : OUTPUT;
            VectorSchemaRoot root = VectorSchemaRoot.create(schema, Allocators.root());
            root.allocateNew();
            ((BigIntVector) root.getVector("n")).setSafe(0, emitted);
            root.setRowCount(1);
            out.emit(root);
            emitted++;
        }
    }
}
