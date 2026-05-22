// Copyright 2025-2026 Query.Farm LLC

package farm.query.vgi.example.table;

import farm.query.vgi.function.ArgSpec;
import farm.query.vgi.function.FunctionMetadata;
import farm.query.vgi.function.FunctionMetadata.OrderPreservation;
import farm.query.vgi.internal.EmitMetadata;
import farm.query.vgi.internal.SchemaUtil;
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
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.List;

/**
 * Deliberately-broken {@code supports_batch_index} fixtures. Each violates the
 * batch_index contract one way; the C++ extension's {@code InstallBatch}
 * raises a typed error on release builds. Mirrors vgi-python's
 * {@code batch_index_broken.py}.
 */
public final class BrokenBatchIndexFunctions {

    private BrokenBatchIndexFunctions() {}

    private static final Schema OUTPUT = Schemas.of(Schemas.nullable("n", Schemas.INT64));
    private static final byte[] OUTPUT_IPC = SchemaUtil.serializeSchema(OUTPUT);

    private static VectorSchemaRoot sequenceRoot(long count) {
        VectorSchemaRoot root = VectorSchemaRoot.create(OUTPUT, Allocators.root());
        root.allocateNew();
        BigIntVector v = (BigIntVector) root.getVector("n");
        for (int i = 0; i < count; i++) v.setSafe(i, i);
        root.setRowCount((int) count);
        return root;
    }

    private abstract static class BrokenBase implements TableFunction {
        @Override public List<ArgSpec> argumentSpecs() {
            return List.of(ArgSpec.positional("count", 0, Schemas.INT64));
        }
        @Override public BindResponse onBind(TableBindParams p) {
            return BindResponse.forSchema(OUTPUT_IPC);
        }
        @Override public long cardinality(TableBindParams p) {
            Object c = p.arguments().positionalAt(0);
            return c instanceof Number n ? n.longValue() : -1L;
        }
        @Override public long maxWorkers() { return 1L; }
        FunctionMetadata brokenMeta(String desc) {
            return FunctionMetadata.describe(desc)
                    .withCategories("testing", "broken")
                    .withOrderPreservation(OrderPreservation.FIXED_ORDER)
                    .withBatchIndex();
        }
    }

    /** Declares supports_batch_index but emits a batch with no vgi_batch_index. */
    public static final class MissingBatchIndexTag extends BrokenBase {
        @Override public String name() { return "broken_missing_batch_index_tag"; }
        @Override public FunctionMetadata metadata() {
            return brokenMeta("DELIBERATELY BROKEN: declares supports_batch_index=True but emits a data batch with no vgi_batch_index metadata. C++ extension's contract check raises.");
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return new OneShotState(((Number) p.arguments().positionalAt(0)).longValue());
        }
        public static final class OneShotState extends TableProducerState {
            public long count;
            public boolean emitted = false;
            public OneShotState() {}
            OneShotState(long count) { this.count = count; }
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (emitted) { out.finish(); return; }
                out.emit(sequenceRoot(count));   // no batch_index metadata — C++ raises
                emitted = true;
            }
        }
    }

    /** Emits batch_index=10 then batch_index=3 on the same stream. */
    public static final class NonMonotoneBatchIndex extends BrokenBase {
        @Override public String name() { return "broken_non_monotone_batch_index"; }
        @Override public FunctionMetadata metadata() {
            return brokenMeta("DELIBERATELY BROKEN: emits batches with strictly decreasing partition_id on one stream. C++ extension's monotonicity check raises (DuckDB's debug-only assertion is not relied upon).");
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return new TwoStepState(((Number) p.arguments().positionalAt(0)).longValue());
        }
        public static final class TwoStepState extends TableProducerState {
            public long count;
            public int step = 0;
            public TwoStepState() {}
            TwoStepState(long count) { this.count = count; }
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (step == 0) {
                    out.emit(sequenceRoot(count), EmitMetadata.batchIndex(10));
                    step = 1;
                } else if (step == 1) {
                    out.emit(sequenceRoot(1), EmitMetadata.batchIndex(3));  // < 10 — C++ raises
                    step = 2;
                } else {
                    out.finish();
                }
            }
        }
    }

    /** Emits a batch_index above DuckDB's per-pipeline cap. */
    public static final class BatchIndexOverflow extends BrokenBase {
        @Override public String name() { return "broken_batch_index_overflow"; }
        @Override public FunctionMetadata metadata() {
            return brokenMeta("DELIBERATELY BROKEN: emits a batch tagged with a partition_id well above DuckDB's BATCH_INCREMENT=10^13 per-pipeline cap. C++ extension rejects at parse time.");
        }
        @Override public TableProducerState createProducer(TableInitParams p) {
            return new OverflowState(((Number) p.arguments().positionalAt(0)).longValue());
        }
        public static final class OverflowState extends TableProducerState {
            public long count;
            public boolean emitted = false;
            public OverflowState() {}
            OverflowState(long count) { this.count = count; }
            @Override public void produceTick(OutputCollector out, CallContext ctx) {
                if (emitted) { out.finish(); return; }
                out.emit(sequenceRoot(count), EmitMetadata.batchIndex(1L << 60));  // > cap — C++ raises
                emitted = true;
            }
        }
    }
}
